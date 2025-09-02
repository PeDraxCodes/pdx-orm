from abc import ABC, abstractmethod
from typing import Any


class DBResult(ABC):

    @property
    @abstractmethod
    def to_list(self) -> list[tuple]:
        ...

    @property
    @abstractmethod
    def to_dict(self) -> list[dict]:
        """
        Converts the SQL query result to a list of dictionaries.

        Note:
            The SQL query has to return a struct or row data.

        Returns:
            list[dict]: A list of dictionaries with the column names as keys and the values of the row as values.
        """
        ...

    @property
    @abstractmethod
    def to_item(self) -> Any | None:
        """
        :returns: None if result is empty
                  else first item of the first row of the result
        """
        ...

    @property
    @abstractmethod
    def to_items(self) -> list:
        """
        :returns: empty list if result is empty
                  else list of first items of each row of the result

        """
        ...
