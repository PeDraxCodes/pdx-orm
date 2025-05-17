import contextvars
import logging
import sqlite3
import traceback
from types import TracebackType

from . import settings
from .logger import ORM_LOGGER_NAME

orm_logger = logging.getLogger(ORM_LOGGER_NAME)

_current_connection_var = contextvars.ContextVar("current_db_connection", default=None)
_transaction_depth_var = contextvars.ContextVar("db_transaction_depth", default=0)


class Connection:
    def __init__(self, foreign_keys: bool = True):
        self.foreign_keys = int(foreign_keys)
        self.conn = None

    def __enter__(self) -> sqlite3.Connection:
        current_depth = _transaction_depth_var.get()
        existing_conn_token = _current_connection_var.get()
        if existing_conn_token:
            self.conn = existing_conn_token
            _transaction_depth_var.set(current_depth + 1)
        else:
            self.conn = sqlite3.connect(settings.DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
            self.conn.execute(f"PRAGMA foreign_keys = {self.foreign_keys}")
            self.conn.execute("BEGIN TRANSACTION")
            _current_connection_var.set(self.conn)
            _transaction_depth_var.set(1)
        return self.conn

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None,
                 exc_tb: TracebackType | None):
        current_depth = _transaction_depth_var.get()

        if current_depth <= 0:
            return

        new_depth = current_depth - 1
        _transaction_depth_var.set(new_depth)
        if new_depth == 0:
            try:
                if exc_type:
                    orm_logger.warning(f"Error: {exc_val}")
                    tb_str = ''.join(traceback.format_tb(exc_tb))
                    orm_logger.error(f"Traceback:\n{tb_str}")
                    orm_logger.warning("Rolling back changes")
                    self.conn.rollback()
                else:
                    self.conn.commit()
            finally:
                _current_connection_var.set(None)
                _transaction_depth_var.set(0)
                self.conn.close()
                self.conn = None
