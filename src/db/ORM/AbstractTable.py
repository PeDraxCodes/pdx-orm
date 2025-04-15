from abc import ABC
from typing import Any, Iterable, Type

from db import Connection
from db.BaseDBOperations import BaseDBOperations, DBResult
from db.ORM import QueryGenerator
from db.ORM.AbstractSchema import AbstractSchema
from db.ORM.BaseData import BaseData
from db.ORM.QueryBuilder import QueryBuilder

type PrimaryKey = list[Any]


class AbstractTable[D: BaseData, K: Iterable](ABC, BaseDBOperations):
    schema: AbstractSchema
    data: Type[BaseData]

    def __init__(self, schema: AbstractSchema, data_class: Type[D]):
        super().__init__()
        self._schema = schema
        self._data_class = data_class

    def get_all(self) -> list[D]:
        query = f"SELECT * FROM {self._schema.table_name};"

        return self.get_data_with_query(query)

    def get_one(self, key: K) -> D:
        """
        Returns a single row based on the provided key.
        """
        if not isinstance(key, list) and not isinstance(key, tuple):
            key = [key]
        # foreign_key = [value for _, value in self._data_class().meta["db_columns"].items() if
        #                value.reference is not None]
        #
        # query = (QueryBuilder()
        # .append(f"SELECT {self._schema.select} FROM " + self._schema.table_name)
        # .append(
        #     QueryGenerator.generate_join(self._schema, [foreign_key[0].field_name], foreign_key[0].reference.schema)))

        query = (
            QueryBuilder()
            .append(f"{self._schema.select} FROM " + self._schema.table_name)
            .append(QueryGenerator.generate_where_with_pk(self._schema, key))
        )
        return self.get_one_with_query(query)

    def insert(self, data: D) -> None:
        """
        Inserts a new row into the table.
        """
        self._insert(data)

    def _insert(self, data: D) -> None:
        columns = data._meta["db_columns"]
        column_names = self._columns_to_insert(data)
        query = (QueryBuilder()
                 .append("INSERT INTO " + self._schema.table_name + " (" + ", ".join(column_names) + ") ")
                 .append("VALUES (" + ", ".join(["?"] * len(column_names)) + ")",
                         [getattr(data, columns[col].field_name) for col in column_names]))

        self.execute(query)

    def _columns_to_insert(self, data: D) -> list[str]:
        """
        Returns the columns to be inserted into the table.
        """
        columns = self._schema.columns
        auto_generated = data._meta["auto_generated"]
        for col in self._schema.columns:
            if col in auto_generated and getattr(data, col) is None:
                columns.remove(col)
        return columns

    def update(self, data: D) -> None:
        """
        Updates a row in the table.
        """
        self._update(data)

    def _update(self, data: D) -> None:
        columns = data._meta["db_columns"]
        pk = self._schema.primaryKey
        column_names = [col for col in self._schema.columns if col not in pk]
        column_values = [getattr(data, columns[col].field_name) for col in column_names]
        query = (QueryBuilder()
                 .append("UPDATE " + self._schema.table_name)
                 .append("SET " + ", ".join([f"{col} = ?" for col in column_names]), column_values)
                 .append(QueryGenerator.generate_where_with_pk(self._schema, [getattr(data, col) for col in
                                                                              self._schema.primaryKey])))

        self.execute(query)

    def delete(self, data: D | K) -> None:
        """
        Deletes a row from the table.
        """

        if isinstance(data, self._data_class):
            pk = [getattr(data, col) for col in self._schema.primaryKey]
        else:
            pk = data
        self._delete(pk)

    def _delete(self, primaryKey: K) -> None:
        query = (QueryBuilder()
                 .append("DELETE FROM " + self._schema.table_name)
                 .append(QueryGenerator.generate_where_with_pk(self._schema, primaryKey)))

        self.execute(query)

    def get_data_with_query(self, query: QueryBuilder | str) -> list[D]:
        """
        Returns a list of data objects based on the provided query.
        """
        foreign_key = [(col, value) for col, value in self._data_class().meta["db_columns"].items() if
                       value.reference is not None]
        result = self.execute_select_query(query).to_dict
        result = [self._data_class(**row) for row in result]
        for col, value in foreign_key:
            foreign_key_values = {getattr(row, value.field_name) for row in result}
            referenced_schema = value.reference.schema
            query = (QueryBuilder()
                     .append(f"SELECT * FROM {referenced_schema.table_name} ")
                     .append(f"WHERE {referenced_schema.primaryKey[0]}")
                     .appendIn(list(foreign_key_values)))

            foreign_key_result = value.reference().get_data_with_query(query)
            foreign_key_map = {getattr(i, referenced_schema.primaryKey[0]): i for i in foreign_key_result}
            for row in result:
                row_value = getattr(row, col)
                if row_value in foreign_key_map:
                    setattr(row, col, foreign_key_map[row_value])
        return result

    def get_one_with_query(self, query: QueryBuilder | str) -> D:
        """
        Returns a single row based on the provided query.
        """
        result = self.execute_select_query(query).to_dict
        data_as_dataclass = self._data_class(**result[0])
        foreign_key = [(col, value) for col, value in data_as_dataclass.meta["db_columns"].items() if
                       value.reference is not None]
        for col, value in foreign_key:
            reference_value = getattr(data_as_dataclass, col)
            resolved_value = value.reference().get_one(reference_value)
            setattr(data_as_dataclass, col, resolved_value)
        return data_as_dataclass

    def execute_select_query(self, query: QueryBuilder | str, params: list | None = None) -> DBResult:
        """
        Executes a SELECT query and returns the result.
        """
        if isinstance(query, QueryBuilder):
            params = query.params
            query = query.query
        return super().execute_select_query(query, params)

    def execute(self, query: QueryBuilder | str, params: list | None = None):
        """
        Executes a query (INSERT, UPDATE, DELETE) and returns the result.
        """
        print(query)

        if isinstance(query, QueryBuilder):
            params = query.params
            query = query.query
        with Connection() as db:
            db.execute(query, params)
