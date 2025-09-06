# Import sqlite3 only if available
try:
    import sqlite3
except ImportError:
    sqlite3 = None

from typing import Any

from .DBResult import DBResult


class SqliteDBResult(DBResult):
    def __init__(self, result: "sqlite3.Cursor"):
        if sqlite3 is None:
            raise ImportError("sqlite3 module is required for SqliteDBResult but not available")
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
    def to_item(self) -> Any | None:
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
