import os
import psycopg
from psycopg.rows import dict_row

_DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/citations",
)


def get_conn():
    return psycopg.connect(_DATABASE_URL, row_factory=dict_row)
