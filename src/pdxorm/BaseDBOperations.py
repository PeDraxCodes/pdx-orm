import logging
import sqlite3

from .DBResult import DBResult
from .logger import ORM_LOGGER_NAME
from .SqliteConnection import SqliteConnection

orm_logger = logging.getLogger(ORM_LOGGER_NAME)


class BaseDBOperations:
    def __init__(self) -> None:
        SqliteConnection.connect()

    def execute_query(self, query: str, params: list | None = None):
        orm_logger.debug(query)
        self.cursor.execute(query, params or [])

    def execute_select_query(self, query: str, params: list | None = None) -> DBResult:
        orm_logger.debug(query, stacklevel=5)
        return DBResult(self.cursor.execute(query, params or []))

    @property
    def cursor(self) -> sqlite3.Cursor:
        return SqliteConnection.cursor

    @property
    def con(self) -> sqlite3.Connection:
        return SqliteConnection.con

    @staticmethod
    def commit():
        SqliteConnection.con.commit()

    @staticmethod
    def rollback():
        SqliteConnection.con.rollback()

    @staticmethod
    def close_connection():
        if SqliteConnection.open:
            SqliteConnection.close()

    @staticmethod
    def connect():
        SqliteConnection.connect()
