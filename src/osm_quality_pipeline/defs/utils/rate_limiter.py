import datetime
import sqlite3
import time

import dagster as dg

logger = dg.get_dagster_logger()

MINUTE = 60
DAY = 86400


def _connect(db_path):
    # isolation_level=None -> autocommit mode, so we control transactions
    # explicitly (needed for BEGIN IMMEDIATE in ApiRateLimiter.acquire()).
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def _ensure_schema(db_path):
    conn = _connect(db_path)
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


class ApiRateLimiter:
    """Caps requests to one API to user-configured N per minute / N per day.

    This is the user's own cap, independent of (and usually stricter than) the
    server-side quota handled by ApiQuotaTracker. Both share the same
    `requests` table: acquire() reserves a row before the request is sent, and
    ApiQuotaTracker.observe() later fills that same row with the quota numbers
    from the response headers, so each request is counted exactly once.

    State is persisted in SQLite (not memory) because asset steps run as
    separate processes under the multiprocess executor. acquire() does its
    check-then-reserve inside a BEGIN IMMEDIATE transaction, which takes
    SQLite's write lock up front so two processes racing to acquire at the
    same instant can't both pass the check before either one records its
    request.
    """

    def __init__(self, db_path, api_name, max_per_minute=None, max_per_day=None):
        self.db_path = db_path
        self.api_name = api_name
        self.max_per_minute = max_per_minute
        self.max_per_day = max_per_day
        _ensure_schema(db_path)

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
        """(remaining_this_minute, remaining_this_day); None means no limit configured.

        A plain (non-locking) read: a momentary race with a concurrent
        acquire() only affects this informational snapshot, never the count
        actually enforced.
        """
        now = time.time()
        conn = _connect(self.db_path)
        try:
            minute_left = (
                max(self.max_per_minute - self._count_since(conn, now - MINUTE), 0)
                if self.max_per_minute is not None
                else None
            )
            day_left = (
                max(self.max_per_day - self._count_since(conn, now - DAY), 0)
                if self.max_per_day is not None
                else None
            )
        finally:
            conn.close()
        return minute_left, day_left

    def log_remaining(self, n_upcoming_requests):
        minute_left, day_left = self.remaining()
        if minute_left is None and day_left is None:
            logger.info(f"[{self.api_name}] no user rate limit configured, processing all {n_upcoming_requests} rows")
            return

        parts = []
        if minute_left is not None:
            parts.append(f"{minute_left}/minute")
        if day_left is not None:
            parts.append(f"{day_left}/day")

        if day_left is None or day_left >= n_upcoming_requests:
            logger.info(f"[{self.api_name}] user rate limit: {', '.join(parts)} remaining for {n_upcoming_requests} rows")
        else:
            logger.info(
                f"[{self.api_name}] user rate limit: {', '.join(parts)} remaining; will process {day_left} of "
                f"{n_upcoming_requests} rows now, then pause and resume automatically once the daily limit frees up"
            )

    def acquire(self):
        """Blocks until a request slot is free, then reserves it.

        Returns the rowid of the reserved row, to be passed to
        ApiQuotaTracker.observe() so it updates this row instead of adding a
        second one for the same request.
        """
        while True:
            now = time.time()
            conn = _connect(self.db_path)
            try:
                conn.execute("BEGIN IMMEDIATE")
                wait_for = 0.0
                if self.max_per_minute is not None and self._count_since(conn, now - MINUTE) >= self.max_per_minute:
                    wait_for = max(wait_for, self._oldest_since(conn, now - MINUTE) + MINUTE - now)
                if self.max_per_day is not None and self._count_since(conn, now - DAY) >= self.max_per_day:
                    wait_for = max(wait_for, self._oldest_since(conn, now - DAY) + DAY - now)

                if wait_for <= 0:
                    cur = conn.execute("INSERT INTO requests (api_name, ts) VALUES (?, ?)", (self.api_name, now))
                    conn.execute("COMMIT")
                    return cur.lastrowid

                conn.execute("COMMIT")  # release the write lock before sleeping
            finally:
                conn.close()

            sleep_for = min(wait_for, 60) + 0.1
            logger.info(f"[{self.api_name}] user rate limit reached, sleeping {sleep_for:.0f}s until a slot frees up")
            time.sleep(sleep_for)


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
        _ensure_schema(db_path)

    def _connect(self):
        return _connect(self.db_path)

    def observe(self, api_name, headers, request_id=None):
        """Call once right after receiving a response (success or error - the
        gateway sets these headers either way). Records the request, warns on
        low quota, and blocks until reset if the server reports it's exhausted.

        If request_id (from ApiRateLimiter.acquire()) is given, that already
        reserved row is updated instead of inserting a new one.
        """
        remaining = headers.get("x-ratelimit-remaining")
        limit = headers.get("x-ratelimit-limit")
        reset = headers.get("x-ratelimit-reset")

        remaining = int(remaining) if remaining is not None else None
        limit = int(limit) if limit is not None else None
        reset = int(reset) if reset is not None else None

        conn = self._connect()
        try:
            if request_id is not None:
                conn.execute(
                    "UPDATE requests SET remaining = ?, quota_limit = ? WHERE rowid = ?",
                    (remaining, limit, request_id),
                )
            else:
                conn.execute(
                    "INSERT INTO requests (api_name, ts, remaining, quota_limit) VALUES (?, ?, ?, ?)",
                    (api_name, time.time(), remaining, limit),
                )
        finally:
            conn.close()

        if remaining is None or limit is None:
            return

        if limit < 0:
            # this is an unlimited key
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
