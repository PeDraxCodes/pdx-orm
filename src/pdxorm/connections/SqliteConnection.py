import logging
import sqlite3

from .AbstractConnection import AbstractConnection
from .. import DBResult, settings
from ..QueryBuilder import QueryBuilder
from ..logger import ORM_LOGGER_NAME
from ..result_objects.SqliteDBResult import SqliteDBResult

orm_logger = logging.getLogger(ORM_LOGGER_NAME)


class SqliteConnection(AbstractConnection):

    def __init__(self, readonly: bool, foreign_keys: bool = True):
        super().__init__(readonly)
        self.foreign_keys = int(foreign_keys)
        self.open = False
        self._con: sqlite3.Connection | None = None
        self.cursor: sqlite3.Cursor | None = None
        self.connect()

    def connect(self):
        if self.open:
            return
        # Try connecting to database
        try:
            path = settings.DB_PATH if not self._readonly else "file:" + settings.DB_PATH + "?mode=ro"
            self._con = sqlite3.connect(path,
                                        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
                                        check_same_thread=False,
                                        uri=True)
            if not self._readonly:
                self._con.execute(f"PRAGMA foreign_keys = {self.foreign_keys}")
                self._con.execute("BEGIN TRANSACTION")

            self.cursor = self._con.cursor()
        except sqlite3.Error as e:
            orm_logger.error("Error while connecting to database: " + str(e))
            self._con = None
            self.cursor = None
            raise e

        self.open = True

    def close(self):
        if self.open:
            self._con.commit()
        self._con.close()
        self._con = None
        self.cursor = None
        self.open = False

    def execute(self, query: QueryBuilder | str, params: list | tuple | None = None) -> DBResult:
        return SqliteDBResult(self._con.execute(self._get_query(query), self._get_params(query, params)))

    def executemany(self, query: QueryBuilder | str, params: list[tuple] | list[list] | None = None) -> DBResult:
        pass

    def executescript(self, script: str) -> DBResult:
        pass

    def rollback(self):
        self._con.rollback()

    def commit(self):
        self._con.commit()

    def ping(self) -> bool:
        # Check if operations are allowed (file not busy)
        try:
            self.cursor.execute('SELECT 1')
            self._con.rollback()
        except sqlite3.OperationalError as e:
            orm_logger.error("Error while connecting to database: " + str(e), exc_info=True)
            return False
        return True
