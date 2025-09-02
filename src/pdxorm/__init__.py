import logging
from urllib.parse import parse_qs, urlparse

from pdxorm.result_objects.DBResult import DBResult  # noqa: F401
from . import (
    QueryGenerator,  # noqa: F401
    settings,
)
from .AbstractSchema import AbstractSchema  # noqa: F401
from .AbstractTable import AbstractTable  # noqa: F401
from .BaseData import BaseData  # noqa: F401
from .Connection import Connection  # noqa: F401
from .ConnectionHandler import ConnectionHandler  # noqa: F401
from .DBColumn import DBColumn  # noqa: F401
from .DatabaseType import DatabaseType
from .QueryBuilder import QueryBuilder  # noqa: F401
from .logger import ORM_LOGGER_NAME  # noqa: F401

orm_logger = logging.getLogger(ORM_LOGGER_NAME)


def setup_database_from_url(database_url: str, database_type: DatabaseType):
    """
    Konfiguriert die Datenbankverbindung über eine URL/DSN.

    Args:
        database_url (str): Z.B. postgresql://user:password@host:port/database?sslmode=require
                                 sqlite:///path/to/database.db
        database_type (DatabaseType): Der Typ der Datenbank (z.B. DatabaseType.MYSQL, DatabaseType.SQLITE)
    """
    if settings.DB_IS_INITIALIZED:
        orm_logger.warning("Datenbank bereits konfiguriert.")
        return

    orm_logger.info("Konfiguriere Datenbank über URL...")
    try:
        parsed_url = urlparse(database_url)
        config = {}
        config['driver'] = parsed_url.scheme
        config['user'] = parsed_url.username
        config['password'] = parsed_url.password
        config['host'] = parsed_url.hostname
        config['port'] = parsed_url.port
        # Pfad für SQLite oder Datenbankname für andere
        config['database'] = parsed_url.path.lstrip('/') if config['driver'] != 'sqlite' else parsed_url.path

        # Extrahiere Query-Parameter (z.B. ?sslmode=require&pool_size=10)
        query_params = parse_qs(parsed_url.query)
        for key, value in query_params.items():
            # Nimm nur den ersten Wert, falls Parameter mehrfach vorkommt
            config[key] = value[0]

        settings.DB_PATH = config['database']  # Nutzt die geparste Config
        orm_logger.info("Datenbankverbindung erfolgreich konfiguriert.")
        settings.DB_IS_INITIALIZED = True
        settings.DB_TYPE = database_type

    except Exception as e:
        orm_logger.error("Fehler beim Parsen der Datenbank-URL oder Konfiguration: %s", e, exc_info=True)
        settings.DB_IS_INITIALIZED = False
        raise
