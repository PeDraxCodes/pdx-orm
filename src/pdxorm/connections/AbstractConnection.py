import logging
from abc import ABC, abstractmethod

from .. import DBResult
from ..QueryBuilder import QueryBuilder
from ..logger import ORM_LOGGER_NAME

orm_logger = logging.getLogger(ORM_LOGGER_NAME)


class AbstractConnection(ABC):
    def __init__(self, readonly: bool):
        self._readonly = readonly
        self.open = False

    @abstractmethod
    def connect(self):
        ...

    @abstractmethod
    def close(self):
        ...

    @abstractmethod
    def rollback(self):
        ...

    @abstractmethod
    def commit(self):
        ...

    @abstractmethod
    def execute(self, query: QueryBuilder | str, params: list | tuple | None = None) -> DBResult:
        ...

    @abstractmethod
    def executemany(self, query: QueryBuilder | str, params: list[tuple] | list[list] | None = None) -> DBResult:
        ...

    @abstractmethod
    def executescript(self, script: str) -> DBResult:
        ...

    @abstractmethod
    def ping(self) -> bool:
        ...

    def log(self, msg: str):
        is_pragma = msg.lower().strip().startswith("pragma") and "key" not in msg.lower()
        is_select = msg.lower().strip().startswith("select")

        if not self._readonly:
            if is_pragma or is_select:
                orm_logger.debug("DML-CONNECTION: %s", msg)  # PRAGMA statements are not logged as DML
            else:
                orm_logger.info("DML-CONNECTION: %s", msg)
        else:
            orm_logger.debug(msg)

    def _get_query(self, query: str | QueryBuilder):
        if isinstance(query, QueryBuilder):
            return query.query
        return query

    def _get_params(self, query: str | QueryBuilder, params: list | None = None):
        if isinstance(query, QueryBuilder):
            return query.params
        return params or []
