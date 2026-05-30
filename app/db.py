import os
import time

import psycopg
from psycopg.rows import dict_row


def get_database_url() -> str:
    return os.environ.get(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/citations",
    )


def get_conn():
    retry_delays = (0.2, 0.5)
    for attempt in range(3):
        try:
            return psycopg.connect(
                get_database_url(),
                row_factory=dict_row,
                connect_timeout=5,
            )
        except psycopg.OperationalError as exc:
            if attempt == 2:
                raise RuntimeError("資料庫暫時無法連線") from exc
            time.sleep(retry_delays[attempt])

    raise RuntimeError("資料庫暫時無法連線")
