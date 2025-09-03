import logging

from . import settings
from .DatabaseType import DatabaseType
from .connections.AbstractConnection import AbstractConnection
from .connections.MySqlConnection import MySqlConnection
from .connections.SqliteConnection import SqliteConnection
from .logger import ORM_LOGGER_NAME

orm_logger = logging.getLogger(ORM_LOGGER_NAME)


class ConnectionHandler:
    _open_connections: list[AbstractConnection] = []
    _read_connection: AbstractConnection | None = None

    @staticmethod
    def get_readonly_connection() -> AbstractConnection:
        if not settings.DB_IS_INITIALIZED:
            raise RuntimeError("Database is not initialized.")

        if ConnectionHandler._read_connection is not None:
            return ConnectionHandler._read_connection

        match settings.DB_TYPE:
            case DatabaseType.SQLITE:
                connection = SqliteConnection(readonly=True)
                ConnectionHandler._read_connection = connection
                ConnectionHandler._open_connections.append(connection)
                return connection
            case DatabaseType.MYSQL:
                connection = MySqlConnection(readonly=True)
                ConnectionHandler._read_connection = connection
                ConnectionHandler._open_connections.append(connection)
                return connection
            case _:
                raise ValueError("Unsupported database type.")

    @staticmethod
    def get_writable_connection(foreign_keys: bool) -> AbstractConnection:
        if not settings.DB_IS_INITIALIZED:
            raise RuntimeError("Database is not initialized.")

        match settings.DB_TYPE:
            case DatabaseType.SQLITE:
                return SqliteConnection(readonly=False, foreign_keys=foreign_keys)
            case DatabaseType.MYSQL:
                return MySqlConnection()
            case _:
                raise ValueError("Unsupported database type.")

    @staticmethod
    def close_all_connections():
        for conn in ConnectionHandler._open_connections:
            conn.close()
        ConnectionHandler._open_connections.clear()
