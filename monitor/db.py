from pathlib import Path
from sqlite3 import Connection
import sqlite3


class Db:
    def __init__(self, db_path: Path):
        self._db_path = db_path

    def init(self):
        conn = self.connect()
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

    def connect(self) -> Connection:
        conn = sqlite3.connect(self._db_path)
        return conn

    def _exec_db(self, sql: str, args: tuple = ()):
        conn = self.connect()
        cur = conn.cursor()
        cur.execute(sql, args)
        conn.commit()
        conn.close()
