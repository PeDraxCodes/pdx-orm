import logging

# Import MySQLdb only if available
try:
    import MySQLdb
except ImportError:
    MySQLdb = None

from .AbstractConnection import AbstractConnection
from .. import DBResult, settings
from ..QueryBuilder import QueryBuilder
from ..logger import ORM_LOGGER_NAME
from ..result_objects.MySqlDBResult import MySqlDBResult

orm_logger = logging.getLogger(ORM_LOGGER_NAME)


class MySqlConnection(AbstractConnection):
    def __init__(self, readonly: bool):
        if MySQLdb is None:
            raise ImportError("MySQLdb module is required for MySqlConnection but not available")
        super().__init__(readonly)
        self._conn: MySQLdb.connections.Connection | None = None
        self._cursor: MySQLdb.cursors.Cursor | None = None
        self.connect()

    def connect(self):
        if self.open:
            return
        try:
            self._conn = MySQLdb.connect(user=settings.DB_USER, password=settings.DB_PASSWORD, host=settings.DB_HOST,
                                         port=settings.DB_PORT,
                                         database=settings.DB_NAME)
            self._cursor = self._conn.cursor()
            if self._readonly:
                with self._conn.cursor() as cursor:
                    cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")

        except MySQLdb.Error as e:
            orm_logger.error("Error while connecting to database: " + str(e))
            self._conn = None
            self._cursor = None
            raise e

        self.open = True

    def close(self):
        if self.open and self._conn is not None:
            self._conn.close()
            self._conn = None
        self.open = False

    def rollback(self):
        if self.open and self._conn is not None:
            self._conn.rollback()

    def commit(self):
        if self.open and self._conn is not None:
            self._conn.commit()

    def execute(self, query: QueryBuilder | str, params: list | tuple | None = None) -> DBResult:
        assert self._cursor is not None
        self.log(
            self._cursor.mogrify(self.replace_placeholder(self._get_query(query)), self._get_params(query, params)))
        self._cursor.execute(self.replace_placeholder(self._get_query(query)), self._get_params(query, params))

        return MySqlDBResult(self._cursor)

    def executemany(self, query: QueryBuilder | str, params: list[tuple] | list[list] | None = None) -> DBResult:
        raise NotImplementedError("MySQL does not support executemany in this ORM implementation.")

    def executescript(self, script: str) -> DBResult:
        raise NotImplementedError("MySQL does not support executescript in this ORM implementation.")

    def ping(self) -> bool:
        try:
            if self._conn is not None:
                self._conn.ping()
            return True
        except MySQLdb.Error:
            return False

    def replace_placeholder(self, query: str) -> str:
        return query.replace("?", "%s")
