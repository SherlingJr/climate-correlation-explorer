import os
from contextlib import contextmanager

from psycopg2.pool import ThreadedConnectionPool

_pool: ThreadedConnectionPool | None = None


def init_pool() -> None:
    global _pool
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL is not set")
    _pool = ThreadedConnectionPool(minconn=1, maxconn=10, dsn=dsn)


def close_pool() -> None:
    if _pool:
        _pool.closeall()


@contextmanager
def get_cursor():
    if _pool is None:
        init_pool()
    conn = _pool.getconn()
    try:
        with conn.cursor() as cur:
            yield cur
        conn.commit()
    finally:
        _pool.putconn(conn)
