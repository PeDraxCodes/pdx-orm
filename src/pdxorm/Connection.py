import logging
import sqlite3
import traceback
from types import TracebackType

from . import settings
from .logger import ORM_LOGGER_NAME

orm_logger = logging.getLogger(ORM_LOGGER_NAME)


class Connection(sqlite3.Connection):

    def __init__(self, *args, **kwargs):
        super().__init__(settings.DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES, *args, **kwargs)

    def __enter__(self):
        self.execute("BEGIN TRANSACTION")
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None,
                 exc_tb: TracebackType | None):
        if exc_type:
            orm_logger.warning(f"Error: {exc_val}")
            tb_str = ''.join(traceback.format_tb(exc_tb))
            orm_logger.error(f"Traceback:\n{tb_str}")
            orm_logger.warning("Rolling back changes")
            self.rollback()
        else:
            self.commit()

        self.close()
