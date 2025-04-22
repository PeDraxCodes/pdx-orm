import logging
import sqlite3

from .DBResult import DBResult
from .logger import ORM_LOGGER_NAME
from .SqliteConnection import SqliteConnection

orm_logger = logging.getLogger(ORM_LOGGER_NAME)


class DBResult:
    def __init__(self, result: sqlite3.Cursor):
        self._result = result

    @property
    def to_list(self) -> list[tuple]:
        return self._result.fetchall()

    @property
    def to_dict(self) -> list[dict]:
        """
        Converts the SQL query result to a list of dictionaries.

        Note:
            The SQL query has to return a struct or row data.

        Returns:
            list[dict]: A list of dictionaries with the column names as keys and the values of the row as values.
        """
        result = self._result.fetchall()
        return [dict(zip([col[0] for col in self._result.description], row)) for row in result]

    @property
    def to_item(self) -> tuple | None:
        """
        :returns: None if result is empty
                  else first item of the first row of the result
        """
        res = self.to_list
        return None if not res else res[0][0]

    @property
    def to_items(self) -> list:
        """
        :returns: empty list if result is empty
                  else list of first items of each row of the result

        """
        return list(map(lambda x: x[0], self.to_list))


class SqliteConnection:
    open = False
    con = None
    cursor = None

    def __init__(self):
        raise RuntimeError("This class is not meant to be instantiated")

    @classmethod
    def connect(cls):
        if cls.open:
            return
        orm_logger.info("Opening database connection")
        # Try connecting to database
        try:
            cls.con = sqlite3.connect("file:" + settings.DB_PATH + "?mode=ro",
                                      detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
                                      check_same_thread=False,
                                      uri=True)
            cls.cursor = cls.con.cursor()
        except sqlite3.Error as e:
            orm_logger.error("Error while connecting to database: " + str(e))
            cls.con = None
            cls.cursor = None
            raise e

        # Check if operations are allowed (file not busy)
        try:
            cls.cursor.execute('BEGIN IMMEDIATE')
            cls.con.rollback()
        except sqlite3.OperationalError as e:
            orm_logger.error("Error while connecting to database: " + str(e))
            cls.con = None
            cls.cursor = None
            raise IOError("Database is busy.")

        cls.open = True

    @classmethod
    def close(cls):
        orm_logger.info("Closing database connection...")
        if cls.open:
            cls.con.commit()
        cls.con.close()
        cls.con = None
        cls.cursor = None
        cls.open = False


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
