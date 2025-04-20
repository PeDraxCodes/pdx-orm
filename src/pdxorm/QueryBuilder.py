from typing import Any, Self


class QueryBuilder:
    def __init__(self):
        self._query = []
        self._params = []

        self._has_from = False
        self._has_where = False

    def append(self, query: str | Self, params: list | Any = None) -> Self:
        """
        Appends a query to the current query.
        """
        if isinstance(query, QueryBuilder):
            return self._append_self(query)

        if params is None:
            params = []
        elif not isinstance(params, list) and not isinstance(params, tuple):
            params = [params]

        self._query.append(query)
        self._params.extend(params)

        if "FROM" in query:
            self._has_from = True

        if "WHERE" in query:
            self._has_where = True

        return self

    def appendWhereOrAnd(self, query: str, params: list | Any = None) -> Self:
        """
        Appends a WHERE or AND clause to the current query.
        """
        if not self._has_where:
            query = "WHERE " + query
            self._has_where = True
        else:
            query = "AND " + query

        return self.append(query, params)

    def appendIn(self, values: list[Any]) -> Self:
        """
        Appends an IN clause to the current query.
        """
        if not self._has_where:
            raise ValueError("IN clause must be preceded by a WHERE clause")
        if not values:
            raise ValueError("IN clause cannot be empty")
        if isinstance(values[0], tuple) or isinstance(values[0], list):
            placeholders = ", ".join(["(" + ", ".join(["?"] * len(v)) + ")" for v in values])
            params = self._flattern(values)
        else:
            placeholders = ", ".join(["?"] * len(values))
            params = values

        query = f"IN ({placeholders})"
        return self.append(query, params)

    def __str__(self) -> str:
        """
        Returns the current query as a string.
        """
        query = " ".join(self._query)
        for param in self._params:
            query = query.replace("?", str(param), 1)
        return query

    def __repr__(self) -> str:
        """
        Returns the current query as a string.
        """
        return str(self)

    def __add__(self, other: object) -> Self:
        """
        Concatenates two QueryBuilder objects.
        """
        if isinstance(other, QueryBuilder):
            return self._append_self(other)
        elif isinstance(other, str):
            return self.append(other)
        else:
            raise TypeError("Unsupported type for addition: " + str(type(other)))

    def _append_self(self, query: Self) -> Self:
        """
        Appends another QueryBuilder object to the current query.
        """
        self._query.extend(query._query)
        self._params.extend(query._params)
        return self

    def _flattern(self, lst: list) -> list:
        """
        Flattens a list of lists into a single list.
        :param lst: The list to flatten.
        :return: A flattened list.
        """
        return [item for sublist in lst for item in sublist]

    @property
    def query(self) -> str:
        """
        Returns the current query as a string.
        """
        return " ".join(self._query)

    @property
    def params(self) -> list[Any]:
        """
        Returns the current parameters as a list.
        """
        return self._params
