import os

import psycopg
from psycopg.rows import dict_row


def get_database_url() -> str:
    return os.environ.get(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/citations",
    )


def get_conn():
    return psycopg.connect(get_database_url(), row_factory=dict_row)
