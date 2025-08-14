from contextlib import contextmanager
from psycopg_pool import ConnectionPool
from app.config import settings
pool = ConnectionPool(conninfo=settings.database_url, max_size=10, kwargs={"autocommit": False})
@contextmanager
def get_conn():
    with pool.connection() as conn:
        yield conn
def execute_many(conn, sql: str, seq_of_params):
    with conn.cursor() as cur:
        cur.executemany(sql, seq_of_params)
def fetch_df(conn, sql: str, params=None):
    import pandas as pd
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        cols = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)
