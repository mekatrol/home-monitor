#!/usr/bin/env python3
import asyncio, os, sqlite3, smtplib, ssl, json, time, subprocess, logging
from email.message import EmailMessage
from pathlib import Path
from typing import Dict, Any, List, Optional
import yaml

# new dep
from asyncio_mqtt import Client, MqttError

BASE = Path(__file__).resolve().parent
CONFIG_PATH = BASE / "devices.yml"
DB_PATH = BASE / "monitor.db"
LOG_PATH = BASE / "monitor.log"

logging.basicConfig(
    filename=str(LOG_PATH),
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


# ---------------- core utils ----------------
def load_config() -> Dict[str, Any]:
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f) or {}


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS device_status (
        name TEXT PRIMARY KEY,
        host TEXT,
        last_seen INTEGER,
        online INTEGER,
        last_change INTEGER,
        details TEXT
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS topic_status (
        topic TEXT PRIMARY KEY,
        last_seen INTEGER,
        stale INTEGER,
        last_change INTEGER,
        max_gap INTEGER,
        pattern TEXT
    )""")
    conn.commit()
    conn.close()


def _exec_db(sql: str, args: tuple = ()):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(sql, args)
    conn.commit()
    conn.close()


def send_email(smtp_cfg: dict, subject: str, body: str):
    if not smtp_cfg or not smtp_cfg.get("smtp_server"):
        return
    msg = EmailMessage()
    msg.set_content(body)
    msg["Subject"] = subject
    msg["From"] = smtp_cfg.get("from")
    msg["To"] = smtp_cfg.get("to")
    ctx = ssl.create_default_context()
    with smtplib.SMTP(
        smtp_cfg["smtp_server"], smtp_cfg.get("smtp_port", 587), timeout=10
    ) as s:
        s.starttls(context=ctx)
        if smtp_cfg.get("username"):
            s.login(smtp_cfg["username"], smtp_cfg["password"])
        s.send_message(msg)


async def send_webhook(url: str, payload: dict):
    data = json.dumps(payload)
    proc = await asyncio.create_subprocess_exec(
        "curl",
        "-sS",
        "-X",
        "POST",
        "-H",
        "Content-Type: application/json",
        "-d",
        data,
        url,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    await proc.communicate()
    if proc.returncode != 0:
        logging.warning("webhook post failed")


async def alert(cfg_alert: dict, subject: str, payload: dict):
    if cfg_alert.get("webhook"):
        try:
            await send_webhook(cfg_alert["webhook"], payload)
        except Exception as e:
            logging.exception("webhook error: %s", e)
    if cfg_alert.get("email", {}).get("smtp_server"):
        try:
            send_email(cfg_alert["email"], subject, json.dumps(payload, indent=2))
        except Exception as e:
            logging.exception("email error: %s", e)


# ---------------- device checks (unchanged) ----------------
async def ping_host(host: str, timeout: int = 4) -> bool:
    proc = await asyncio.create_subprocess_exec(
        "ping",
        "-c",
        "1",
        "-W",
        str(timeout),
        host,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        await asyncio.wait_for(proc.communicate(), timeout=timeout + 2)
    except asyncio.TimeoutError:
        proc.kill()
        return False
    return proc.returncode == 0


async def check_tcp_port(host: str, port: int, timeout: float = 3.0) -> bool:
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=timeout
        )
        writer.close()
        try:
            await writer.wait_closed()
        except AttributeError:
            pass
        return True
    except Exception:
        return False


def update_device_db(
    name: str, host: str, online: bool, details: dict
) -> Optional[int]:
    ts = int(time.time())
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT online,last_change FROM device_status WHERE name=?", (name,))
    row = cur.fetchone()
    prev = row[0] if row else None
    last_change = row[1] if row else ts
    if prev is None:
        cur.execute(
            """INSERT INTO device_status(name,host,last_seen,online,last_change,details)
                       VALUES(?,?,?,?,?,?)""",
            (name, host, ts if online else None, int(online), ts, json.dumps(details)),
        )
    else:
        if prev != int(online):
            last_change = ts
        cur.execute(
            """UPDATE device_status SET host=?,last_seen=?,online=?,last_change=?,details=?
                       WHERE name=?""",
            (
                host,
                ts if online else None,
                int(online),
                last_change,
                json.dumps(details),
                name,
            ),
        )
    conn.commit()
    conn.close()
    return prev


async def check_device(device: dict, cfg_alert: dict):
    name, host = device.get("name"), device.get("host")
    ports: List[int] = device.get("ports", [])
    ping_ok = await ping_host(host)
    port_res = (
        await asyncio.gather(*(check_tcp_port(host, int(p), 2.0) for p in ports))
        if ports
        else []
    )
    online = ping_ok or any(port_res)
    details = {"ping": ping_ok, "ports": {str(p): r for p, r in zip(ports, port_res)}}
    prev = update_device_db(name, host, online, details)
    if prev is not None and prev != int(online):
        payload = {
            "type": "device_change",
            "device": name,
            "host": host,
            "online": bool(online),
            "time": int(time.time()),
        }
        await alert(
            cfg_alert, f"[Monitor] {name} {'ONLINE' if online else 'OFFLINE'}", payload
        )


# ---------------- MQTT monitoring ----------------
class MqttWatcher:
    def __init__(self, cfg_mqtt: dict, cfg_alert: dict):
        self.cfg = cfg_mqtt or {}
        self.alert_cfg = cfg_alert or {}
        self.last_seen: Dict[str, int] = {}  # concrete topic -> ts
        self.stale: Dict[str, int] = {}  # concrete topic -> 0/1
        self.patterns: List[Dict[str, Any]] = self.cfg.get("topics", [])
        self.start_ts = int(time.time())

    async def run(self):
        if not self.cfg:
            logging.info("mqtt disabled")
            return
        host = self.cfg.get("host", "127.0.0.1")
        port = int(self.cfg.get("port", 1883))
        cid = self.cfg.get("client_id", "home-monitor")
        keepalive = int(self.cfg.get("keepalive", 30))
        username = self.cfg.get("username") or None
        password = self.cfg.get("password") or None
        tls = bool(self.cfg.get("tls", False))

        while True:
            try:
                ssl_ctx = ssl.create_default_context() if tls else None
                async with Client(
                    hostname=host,
                    port=port,
                    username=username,
                    password=password,
                    client_id=cid,
                    keepalive=keepalive,
                    ssl=ssl_ctx,
                ) as client:
                    # subscribe to all patterns
                    for p in self.patterns:
                        t = p.get("topic")
                        if t:
                            await client.subscribe(t)
                    logging.info("mqtt connected and subscribed")
                    tasks = [
                        asyncio.create_task(self._consume(client)),
                        asyncio.create_task(self._evaluate_loop()),
                    ]
                    await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
            except MqttError as e:
                logging.warning("mqtt error: %s", e)
            except Exception as e:
                logging.exception("mqtt exception: %s", e)
            # backoff on disconnect
            await asyncio.sleep(5)

    async def _consume(self, client: Client):
        async with client.unfiltered_messages() as messages:
            for p in self.patterns:
                # already subscribed in run()
                pass
            async for msg in messages:
                t = msg.topic
                ts = int(time.time())
                self.last_seen[t] = ts
                # ensure row exists
                conn = sqlite3.connect(DB_PATH)
                cur = conn.cursor()
                # find max_gap from first matching pattern
                mg = self._max_gap_for_topic(t)
                cur.execute(
                    """INSERT OR IGNORE INTO topic_status(topic,last_seen,stale,last_change,max_gap,pattern)
                               VALUES(?,?,?,?,?,?)""",
                    (t, ts, 0, ts, mg, self._first_pattern_for_topic(t)),
                )
                cur.execute(
                    """UPDATE topic_status SET last_seen=?, max_gap=?, pattern=? WHERE topic=?""",
                    (ts, mg, self._first_pattern_for_topic(t), t),
                )
                conn.commit()
                conn.close()

    async def _evaluate_loop(self):
        while True:
            await asyncio.sleep(5)
            now = int(time.time())
            startup_grace = int(self.cfg.get("startup_grace", 120))
            # evaluate all known topics
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute("SELECT topic,last_seen,stale,max_gap FROM topic_status")
            rows = cur.fetchall()
            for topic, last_seen, stale, max_gap in rows:
                # topics never seen: treat per startup_grace
                last_seen = last_seen or 0
                gap = now - last_seen if last_seen else None
                should_stale = (
                    1
                    if (last_seen and gap > max_gap)
                    or (not last_seen and (now - self.start_ts) > startup_grace)
                    else 0
                )
                if should_stale != stale:
                    cur.execute(
                        "UPDATE topic_status SET stale=?, last_change=? WHERE topic=?",
                        (should_stale, now, topic),
                    )
                    conn.commit()
                    state = "STALE" if should_stale else "RESUMED"
                    payload = {
                        "type": "mqtt_gap_change",
                        "topic": topic,
                        "state": state,
                        "last_seen": last_seen,
                        "gap_sec": (gap if gap is not None else None),
                        "time": now,
                    }
                    subj = f"[Monitor] MQTT {topic} {state}"
                    await alert(self.alert_cfg, subj, payload)
            conn.close()

    def _first_pattern_for_topic(self, topic: str) -> str:
        # simple match using MQTT wildcards
        for p in self.patterns:
            pat = p.get("topic", "")
            if _mqtt_match(pat, topic):
                return pat
        return ""

    def _max_gap_for_topic(self, topic: str) -> int:
        for p in self.patterns:
            if _mqtt_match(p.get("topic", ""), topic):
                return int(p.get("max_gap", 120))
        return 120


# MQTT wildcard matcher (‘+’ and ‘#’)
def _mqtt_match(pattern: str, topic: str) -> bool:
    ps = pattern.split("/")
    ts = topic.split("/")
    i = j = 0
    while i < len(ps) and j < len(ts):
        if ps[i] == "#":
            return True
        if ps[i] != "+" and ps[i] != ts[j]:
            return False
        i += 1
        j += 1
    if i == len(ps) and j == len(ts):
        return True
    if i == len(ps) - 1 and ps[i] == "#":
        return True
    return False


# ---------------- main loop ----------------
async def device_loop():
    cfg = load_config()
    interval = int(cfg.get("check_interval", 30))
    while True:
        start = time.time()
        for d in cfg.get("devices", []):
            await check_device(d, cfg.get("alert", {}))
        # hot-reload config safely
        try:
            cfg = load_config()
        except Exception as e:
            logging.exception("config reload error: %s", e)
        elapsed = time.time() - start
        await asyncio.sleep(max(1, interval - elapsed))


async def mqtt_loop():
    cfg = load_config()
    watcher = MqttWatcher(cfg.get("mqtt", {}), cfg.get("alert", {}))
    await watcher.run()


def main():
    init_db()
    logging.info("monitor start")
    try:
        asyncio.run(
            asyncio.wait(
                {asyncio.create_task(device_loop()), asyncio.create_task(mqtt_loop())},
                return_when=asyncio.FIRST_EXCEPTION,
            )
        )
    except KeyboardInterrupt:
        logging.info("monitor stop")
    except Exception:
        logging.exception("monitor crash")


if __name__ == "__main__":
    main()
