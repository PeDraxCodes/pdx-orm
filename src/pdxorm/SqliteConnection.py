import logging
import sqlite3

from . import settings
from .logger import ORM_LOGGER_NAME

orm_logger = logging.getLogger(ORM_LOGGER_NAME)


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
