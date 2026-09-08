import datetime
import sqlite3
import time

import dagster as dg

logger = dg.get_dagster_logger()


class ApiQuotaTracker:
    """Tracks HeiGIT's own per-key quota, reported via `x-ratelimit-*` response
    headers on every request. That quota is shared across all APIs behind the
    same gateway key (ohsome-quality-api and ohsome-api both draw from the same
    pool), so this is one tracker for both, not one per API.

    Unlike a locally-guessed cap, this reacts to the server's own authoritative
    numbers: it warns once quota gets low, and pauses (sleeping until the
    server-reported reset time) once it's actually exhausted, instead of
    hammering the API into repeated 403s. It also keeps a history log of every
    request (timestamp + api_name) for later "how many requests per hour" style
    reporting - something the live headers alone can't answer, since they only
    ever describe the current moment.
    """

    def __init__(self, db_path, warn_threshold_ratio=0.05):
        self.db_path = db_path
        self.warn_threshold_ratio = warn_threshold_ratio
        self._warned_low = False
        self._last_reset = None

        conn = self._connect()
        try:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS requests "
                "(api_name TEXT NOT NULL, ts REAL NOT NULL, remaining INTEGER, quota_limit INTEGER)"
            )
            # migrate tables created by the older ApiRateLimiter, which only had (api_name, ts)
            existing_columns = {row[1] for row in conn.execute("PRAGMA table_info(requests)").fetchall()}
            if "remaining" not in existing_columns:
                conn.execute("ALTER TABLE requests ADD COLUMN remaining INTEGER")
            if "quota_limit" not in existing_columns:
                conn.execute("ALTER TABLE requests ADD COLUMN quota_limit INTEGER")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_requests_api_ts ON requests(api_name, ts)")
        finally:
            conn.close()

    def _connect(self):
        conn = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def observe(self, api_name, headers):
        """Call once right after receiving a response (success or error - the
        gateway sets these headers either way). Records the request, warns on
        low quota, and blocks until reset if the server reports it's exhausted.
        """
        remaining = headers.get("x-ratelimit-remaining")
        limit = headers.get("x-ratelimit-limit")
        reset = headers.get("x-ratelimit-reset")

        remaining = int(remaining) if remaining is not None else None
        limit = int(limit) if limit is not None else None
        reset = int(reset) if reset is not None else None

        conn = self._connect()
        try:
            conn.execute(
                "INSERT INTO requests (api_name, ts, remaining, quota_limit) VALUES (?, ?, ?, ?)",
                (api_name, time.time(), remaining, limit),
            )
        finally:
            conn.close()

        if remaining is None or limit is None:
            return

        if reset != self._last_reset:
            self._last_reset = reset
            self._warned_low = False

        if remaining <= 0:
            if reset is not None:
                wait_for = max(reset - time.time(), 0) + 1
                logger.warning(f"[{api_name}] API quota exhausted (0/{limit}); sleeping {wait_for:.0f}s until reset")
                time.sleep(wait_for)
            return

        if not self._warned_low and remaining <= limit * self.warn_threshold_ratio:
            reset_str = (
                datetime.datetime.fromtimestamp(reset, tz=datetime.timezone.utc).isoformat()
                if reset is not None
                else "unknown"
            )
            logger.warning(f"[{api_name}] API quota low: {remaining}/{limit} remaining, resets at {reset_str}")
            self._warned_low = True

    def hourly_counts(self, since_hours=48, api_name=None):
        since = time.time() - since_hours * 3600
        query = "SELECT strftime('%Y-%m-%d %H:00', ts, 'unixepoch', 'localtime') AS hour, COUNT(*) FROM requests WHERE ts > ?"
        params = [since]
        if api_name is not None:
            query += " AND api_name = ?"
            params.append(api_name)
        query += " GROUP BY hour ORDER BY hour"

        conn = self._connect()
        try:
            return conn.execute(query, params).fetchall()
        finally:
            conn.close()
