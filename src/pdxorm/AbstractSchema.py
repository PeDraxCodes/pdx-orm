from abc import ABC, abstractmethod


class AbstractSchema(ABC):

    def __init__(self, alias: str):
        self._alias = alias

    @property
    def alias(self) -> str:
        return self._alias or ""

    def _alias_internal(self) -> str:
        if self._alias:
            return f"{self._alias}."
        return ""

    def _alias_external(self) -> str:
        if self._alias:
            return f" AS {self._alias}"
        return ""

    @property
    @abstractmethod
    def table_name(self):
        ...

    @property
    @abstractmethod
    def table_name_no_alias(self):
        ...

    @property
    @abstractmethod
    def select(self):
        ...

    @property
    @abstractmethod
    def columns(self) -> list[str]:
        ...

    @property
    @abstractmethod
    def primaryKey(self) -> list[str]:
        ...

    @classmethod
    def no_alias(cls) -> "AbstractSchema":
        return cls("")
