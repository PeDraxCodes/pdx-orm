from typing import Any

import MySQLdb.cursors

from pdxorm import DBResult


class MySqlDBResult(DBResult):
    def __init__(self, cursor: MySQLdb.cursors.Cursor):
        self._cursor = cursor

    @property
    def to_list(self) -> list[tuple]:
        return self._cursor.fetchall()

    @property
    def to_dict(self) -> list[dict]:
        result = self._cursor.fetchall()
        return [dict(zip([col[0] for col in self._cursor.description], row)) for row in result]

    @property
    def to_item(self) -> Any | None:
        res = self.to_list
        return None if not res else res[0][0]

    @property
    def to_items(self) -> list:
        return list(map(lambda x: x[0], self.to_list))
