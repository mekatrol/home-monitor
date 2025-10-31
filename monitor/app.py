#!/usr/bin/env python3
import asyncio
import json
import sys
import time
import logging
from pathlib import Path
from typing import List, Optional
import subprocess

# Our modules
from config_loader import load_config
from db import Db
from mqtt_watcher import MqttWatcher
from alerting import alert

if sys.platform.startswith("win"):
    # Needed because ProactorEventLoop lacks add_reader/add_writer
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

BASE = Path(__file__).resolve().parent
CONFIG_BASE_PATH = BASE / "devices.yml"
CONFIG_DEBUG_PATH = BASE / "devices.debug.yml"
DB_PATH = BASE / "monitor.db"
LOG_PATH = BASE / "monitor.log"

db = Db(DB_PATH)

logging.basicConfig(
    filename=str(LOG_PATH),
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


async def ping_host(host: str, timeout: int = 4) -> bool:
    """
    Cross-platform ping that works with Windows SelectorEventLoop.
    Runs blocking subprocess in a worker thread.
    """
    if sys.platform.startswith("win"):
        args = ["ping", "-n", "1", "-w", str(int(timeout * 1000)), host]
    else:
        args = ["ping", "-c", "1", "-W", str(timeout), host]

    def _run() -> bool:
        try:
            r = subprocess.run(
                args,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=timeout + 2,
            )
            return r.returncode == 0
        except Exception:
            return False

    return await asyncio.to_thread(_run)


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
    except Exception as e:
        logging.exception("email error: %s", e)
        return False


def update_device_db(
    name: str, host: str, online: bool, details: dict
) -> Optional[int]:
    ts = int(time.time())
    conn = db.connect()
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

    if host is None or name is None:
        return

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


async def device_loop():
    cfg = load_config(CONFIG_BASE_PATH, CONFIG_DEBUG_PATH)
    interval = int(cfg.get("check_interval", 30))
    while True:
        start = time.time()
        for d in cfg.get("devices", []):
            await check_device(d, cfg.get("alert", {}))
        # hot-reload config safely
        try:
            cfg = load_config(CONFIG_BASE_PATH, CONFIG_DEBUG_PATH)
        except Exception as e:
            logging.exception("config reload error: %s", e)
        elapsed = time.time() - start
        await asyncio.sleep(max(1, interval - elapsed))


async def mqtt_loop():
    cfg = load_config(CONFIG_BASE_PATH, CONFIG_DEBUG_PATH)
    watcher = MqttWatcher(db, cfg.get("mqtt", {}), cfg.get("alert", {}))
    await watcher.run()


async def _main():
    db.init()
    logging.info("monitor start")
    await asyncio.gather(
        device_loop(),
        mqtt_loop(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        logging.info("monitor stop")
    except Exception as e:
        logging.exception("monitor crash", e)
