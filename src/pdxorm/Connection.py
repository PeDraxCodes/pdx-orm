import contextvars
import logging
import traceback
from types import TracebackType

from pdxorm.connections.AbstractConnection import AbstractConnection
from .ConnectionHandler import ConnectionHandler
from .logger import ORM_LOGGER_NAME

orm_logger = logging.getLogger(ORM_LOGGER_NAME)

_current_connection_var = contextvars.ContextVar("current_db_connection", default=None)
_transaction_depth_var = contextvars.ContextVar("db_transaction_depth", default=0)


class Connection:
    def __init__(self, foreign_keys: bool = True):
        self.foreign_keys = foreign_keys
        self.conn: AbstractConnection | None = None

    def __enter__(self) -> AbstractConnection:
        current_depth = _transaction_depth_var.get()
        existing_conn_token = _current_connection_var.get()
        if existing_conn_token:
            self.conn = existing_conn_token
            _transaction_depth_var.set(current_depth + 1)
        else:
            self.conn = ConnectionHandler.get_writable_connection(self.foreign_keys)
            _current_connection_var.set(self.conn)  # noqa
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
