from abc import ABC
from typing import Any, Iterable, Type

from db import Connection
from db.BaseDBOperations import BaseDBOperations, DBResult
from db.DBColumn import DBColumn
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

    def get_one(self, key: K, nullable: bool = False) -> D | None:
        """
        Returns a single row based on the provided key.
        """
        if not isinstance(key, list) and not isinstance(key, tuple):
            key = [key]
        query = QueryGenerator.generate_query_with_pk(self._schema, key)
        result = self.get_one_or_none_with_query(query)
        if not result and not nullable:
            raise ValueError(f"No data found for key: {key}")
        return result

    def insert(self, data: D) -> None:
        """
        Inserts a new row into the table.
        """
        self._insert(data)

    def _insert(self, data: D) -> None:
        columns = data._meta.db_columns
        column_names = self._columns_to_insert(data)
        query = (QueryBuilder()
                 .append("INSERT INTO " + self._schema.table_name + " (" + ", ".join(column_names) + ") ")
                 .append("VALUES (" + ", ".join(["?"] * len(column_names)) + ")",
                         data.get_values_for_columns([columns[col].field_name for col in column_names])))

        self.execute(query)

    def _columns_to_insert(self, data: D) -> list[str]:
        """
        Returns the columns to be inserted into the table.
        """
        columns = self._schema.columns
        auto_generated = data._meta.auto_generated_fields
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
        columns = data._meta.db_columns
        pk = self._schema.primaryKey
        column_names = [col for col in self._schema.columns if col not in pk]
        field_names = [columns[col].field_name for col in column_names]
        attr = data.get_values_for_columns(field_names)
        query = (QueryBuilder()
                 .append("UPDATE " + self._schema.table_name)
                 .append("SET " + ", ".join([f"{col} = ?" for col in column_names]), attr)
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
        foreign_key = self._data_class.meta().foreign_keys.items()
        result = self.execute_select_query(query).to_dict
        for col, values in foreign_key:
            foreign_key_values = {self._get_fk_as_tuple(row, values) for row in result}
            referenced_schema = values[0].reference().schema
            query = (QueryBuilder()
                     .append(f"SELECT * FROM {referenced_schema.table_name} ")
                     .append(f"WHERE ({', '.join(referenced_schema.primaryKey)})")
                     .appendIn(list(foreign_key_values)))

            foreign_key_result = values[0].reference().get_data_with_query(query)
            foreign_primary_key = referenced_schema.primaryKey
            lookup_map = {}

            if len(foreign_primary_key) > 1:
                for i in foreign_key_result:
                    lookup_map[i.flattened_primary_key] = i
                for row in result:
                    row_value = self._get_fk_as_tuple(row, values)
                    if row_value in lookup_map:
                        row[col] = lookup_map[row_value]
                    else:
                        row[col] = None
            else:
                foreign_key_map = {getattr(i, referenced_schema.primaryKey[0]): i for i in foreign_key_result}
                for row in result:
                    row_value = row[col]
                    if row_value in foreign_key_map:
                        row[col] = foreign_key_map[row_value]

        return [self._data_class(**row) for row in result]

    def get_one_with_query(self, query: QueryBuilder | str) -> D:
        """
        Returns a single row based on the provided query.
        """
        result = self.get_one_or_none_with_query(query)
        if not result:
            raise ValueError("No data found")
        return result

    def get_one_or_none_with_query(self, query: QueryBuilder | str) -> D | None:
        """
        Returns a single row based on the provided query.
        """
        result = self.execute_select_query(query).to_dict
        if not result:
            return None
        data_as_dataclass = result[0]
        for col, values in self._data_class().meta().foreign_keys.items():
            key = self._get_fk_as_tuple(data_as_dataclass, values)
            resolved_value = values[0].reference().get_one(key, nullable=True)
            data_as_dataclass[col] = resolved_value
        return self._data_class(**data_as_dataclass)

    @staticmethod
    def _get_fk_as_tuple(result: dict, fk: list[DBColumn]) -> tuple:
        key = []
        for value in fk:
            key.append(result[value.field_name])
        return tuple(key)

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
