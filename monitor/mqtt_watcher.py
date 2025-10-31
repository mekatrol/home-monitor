import asyncio
import logging
import time
from typing import Any, Dict, List
from aiomqtt import Client, MqttError, TLSParameters

from db import Db
from alerting import alert


class MqttWatcher:
    def __init__(self, db: Db, cfg_mqtt: dict, cfg_alert: dict):
        self.db = db
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
        keep = int(self.cfg.get("keepalive", 30))
        user = self.cfg.get("username") or None
        pwd = self.cfg.get("password") or None
        use_tls = bool(self.cfg.get("tls", False))
        tls_params = TLSParameters() if use_tls else None

        while True:
            try:
                async with Client(
                    host,
                    port=port,
                    username=user,
                    password=pwd,
                    keepalive=keep,
                    tls_params=tls_params,
                ) as client:
                    for pattern in self.patterns:
                        topic = pattern.get("topic")
                        if topic:
                            await client.subscribe(topic)
                            logging.info(
                                f"mqtt connected: '{host}' and subscribed: '{topic}'"
                            )

                    tasks = [
                        asyncio.create_task(self._consume(client)),
                        asyncio.create_task(self._evaluate_loop()),
                    ]
                    await asyncio.gather(*tasks)
            except MqttError as e:
                logging.warning("mqtt error: %s", e)
                await asyncio.sleep(5)

    async def _consume(self, client: Client):
        async for msg in client.messages:
            t = str(msg.topic)  # <- fix
            ts = int(time.time())
            self.last_seen[t] = ts

            mg = self._max_gap_for_topic(t)
            pat = self._first_pattern_for_topic(t)

            conn = self.db.connect()
            cur = conn.cursor()
            cur.execute(
                """INSERT OR IGNORE INTO topic_status(topic,last_seen,stale,last_change,max_gap,pattern)
                        VALUES(?,?,?,?,?,?)""",
                (t, ts, 0, ts, mg, pat),
            )
            cur.execute(
                """UPDATE topic_status SET last_seen=?, max_gap=?, pattern=? WHERE topic=?""",
                (ts, mg, pat, t),
            )
            conn.commit()
            conn.close()

    async def _evaluate_loop(self):
        while True:
            await asyncio.sleep(5)
            now = int(time.time())
            startup_grace = int(self.cfg.get("startup_grace", 120))
            # evaluate all known topics
            conn = self.db.connect()
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
