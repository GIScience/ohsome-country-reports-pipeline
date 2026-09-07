import sqlite3
import time

import dagster as dg

logger = dg.get_dagster_logger()

HOUR = 3600
DAY = 86400


class ApiRateLimiter:
    """Caps requests to an external API to N per hour / N per day.

    State is persisted in SQLite (not memory) because asset steps run as
    separate processes under the multiprocess executor, and nothing at the
    Dagster level currently serializes concurrent runs. acquire() therefore
    does its check-then-reserve inside a BEGIN IMMEDIATE transaction, which
    takes SQLite's write lock up front so two processes racing to acquire at
    the same instant can't both pass the check before either one records its
    request.
    """

    def __init__(self, db_path, api_name, max_per_hour=None, max_per_day=None):
        self.db_path = db_path
        self.api_name = api_name
        self.max_per_hour = max_per_hour
        self.max_per_day = max_per_day
        conn = self._connect()
        try:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS requests (api_name TEXT NOT NULL, ts REAL NOT NULL)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_requests_api_ts ON requests(api_name, ts)"
            )
        finally:
            conn.close()

    def _connect(self):
        # isolation_level=None -> autocommit mode, so we control transactions
        # explicitly (needed for BEGIN IMMEDIATE in acquire()).
        conn = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _count_since(self, conn, since):
        return conn.execute(
            "SELECT COUNT(*) FROM requests WHERE api_name = ? AND ts > ?",
            (self.api_name, since),
        ).fetchone()[0]

    def _oldest_since(self, conn, since):
        return conn.execute(
            "SELECT MIN(ts) FROM requests WHERE api_name = ? AND ts > ?",
            (self.api_name, since),
        ).fetchone()[0]

    def remaining(self):
        """(remaining_this_hour, remaining_this_day); None means no limit configured.

        A plain (non-locking) read: a momentary race with a concurrent
        acquire() only affects this informational snapshot, never the count
        actually enforced.
        """
        now = time.time()
        conn = self._connect()
        try:
            hour_left = (
                max(self.max_per_hour - self._count_since(conn, now - HOUR), 0)
                if self.max_per_hour is not None
                else None
            )
            day_left = (
                max(self.max_per_day - self._count_since(conn, now - DAY), 0)
                if self.max_per_day is not None
                else None
            )
        finally:
            conn.close()
        return hour_left, day_left

    def log_remaining(self, n_upcoming_requests):
        hour_left, day_left = self.remaining()
        if hour_left is None and day_left is None:
            logger.info(f"[{self.api_name}] no rate limit configured, processing all {n_upcoming_requests} rows")
            return

        limits = [x for x in (hour_left, day_left) if x is not None]
        free_now = min(limits)
        parts = []
        if hour_left is not None:
            parts.append(f"{hour_left}/hour")
        if day_left is not None:
            parts.append(f"{day_left}/day")

        if free_now >= n_upcoming_requests:
            logger.info(f"[{self.api_name}] {', '.join(parts)} remaining, enough to process all {n_upcoming_requests} rows")
        else:
            logger.info(
                f"[{self.api_name}] {', '.join(parts)} remaining; will process {free_now} of "
                f"{n_upcoming_requests} rows now, then pause and resume automatically once the limit frees up"
            )

    def acquire(self):
        """Blocks until a request slot is free, then reserves it.

        Safe under concurrent processes: the check and the reservation happen
        inside one BEGIN IMMEDIATE transaction, so only one caller at a time
        (across all processes) can be evaluating "is there room" at once.
        """
        while True:
            now = time.time()
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                wait_for = 0.0
                if self.max_per_hour is not None and self._count_since(conn, now - HOUR) >= self.max_per_hour:
                    wait_for = max(wait_for, self._oldest_since(conn, now - HOUR) + HOUR - now)
                if self.max_per_day is not None and self._count_since(conn, now - DAY) >= self.max_per_day:
                    wait_for = max(wait_for, self._oldest_since(conn, now - DAY) + DAY - now)

                if wait_for <= 0:
                    conn.execute("INSERT INTO requests (api_name, ts) VALUES (?, ?)", (self.api_name, now))
                    conn.execute("COMMIT")
                    return

                conn.execute("COMMIT")  # release the write lock before sleeping
            finally:
                conn.close()

            sleep_for = min(wait_for, 60) + 0.1
            logger.info(f"[{self.api_name}] rate limit reached, sleeping {sleep_for:.0f}s until a slot frees up")
            time.sleep(sleep_for)
