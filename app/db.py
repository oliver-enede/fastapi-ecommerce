import sqlite3
from pathlib import Path
from typing import Optional, Tuple

# Database file path
DB_PATH = Path("data/transactions.db")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL,
    product_id INTEGER,
    timestamp TEXT NOT NULL,
    transaction_amount REAL NOT NULL
);
"""

CREATE_INDEX_SQL = "CREATE INDEX IF NOT EXISTS idx_user_time ON transactions(user_id, timestamp);"


def get_conn():
    conn = sqlite3.connect(str(DB_PATH), detect_types=sqlite3.PARSE_DECLTYPES)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_db(replace: bool = False):
    """Initialize the database (drop + recreate table if replace=True)."""
    conn = get_conn()
    cur = conn.cursor()
    if replace:
        cur.execute("DROP TABLE IF EXISTS transactions;")
    cur.execute(CREATE_TABLE_SQL)
    cur.execute(CREATE_INDEX_SQL)
    conn.commit()
    conn.close()


def insert_many(rows):
    """Insert many rows into the transactions table."""
    conn = get_conn()
    cur = conn.cursor()
    sql = """
        INSERT OR IGNORE INTO transactions
        (transaction_id, user_id, product_id, timestamp, transaction_amount)
        VALUES (?, ?, ?, ?, ?);
    """
    cur.executemany(sql, rows)
    conn.commit()
    inserted = cur.rowcount if cur.rowcount != -1 else len(rows)
    conn.close()
    return inserted


def query_summary(
    user_id: int, start: Optional[str], end: Optional[str]
) -> Tuple[int, Optional[float], Optional[float], Optional[float]]:
    """Return (count, min, max, avg) for a given user and optional date range."""
    conn = get_conn()
    cur = conn.cursor()
    params = [user_id]
    where = "user_id = ?"

    if start and end:
        where += " AND timestamp BETWEEN ? AND ?"
        params += [start, end]
    elif start:
        where += " AND timestamp >= ?"
        params.append(start)
    elif end:
        where += " AND timestamp <= ?"
        params.append(end)

    sql = f"""
        SELECT COUNT(*), MIN(transaction_amount), MAX(transaction_amount), AVG(transaction_amount)
        FROM transactions
        WHERE {where};
    """
    cur.execute(sql, params)
    result = cur.fetchone()
    conn.close()

    if result:
        return result  # (count, min, max, avg)
    return (0, None, None, None)
