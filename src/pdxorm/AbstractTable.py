import logging
from abc import ABC
from typing import Any, Type

from . import QueryGenerator
from .AbstractSchema import AbstractSchema
from .BaseDBOperations import BaseDBOperations
from .BaseData import BaseData
from .Connection import Connection
from .DBColumn import DBColumn
from .DBResult import DBResult
from .OrmEnums import FetchType
from .QueryBuilder import QueryBuilder
from .logger import ORM_LOGGER_NAME

PrimaryKey = list[Any]

orm_logger = logging.getLogger(ORM_LOGGER_NAME)


class AbstractTable[D: BaseData, K](ABC, BaseDBOperations):
    schema: AbstractSchema
    dataclass: Type[BaseData]

    def __init__(self):
        super().__init__()

    def get_all(self, fetch_type: FetchType = FetchType.EAGER) -> list[D]:
        query = f"SELECT * FROM {self.schema.table_name};"

        return self.get_data_with_query(query, fetch_type)

    def get_one(self, key: K, nullable: bool = False, fetch_type: FetchType = FetchType.EAGER) -> D | None:
        """
        Returns a single row based on the provided key.
        """
        if not isinstance(key, list) and not isinstance(key, tuple):
            key = [key]
        query = QueryGenerator.generate_query_with_pk(self.schema, key)
        result = self.get_one_or_none_with_query(query, fetch_type)
        if not result and not nullable:
            raise ValueError(f"No data found for key: {key}")
        return result

    def insert(self, data: D) -> None:
        """
        Inserts a new row into the table.
        """
        self._insert(data)

    def _insert(self, data: D) -> None:
        column_names = self._columns_to_insert(data)
        query = (QueryBuilder()
                 .append("INSERT INTO " + self.schema.table_name + " (" + ", ".join(column_names) + ") ")
                 .append("VALUES (" + ", ".join(["?"] * len(column_names)) + ")",
                         data.get_values_for_columns(column_names)))

        self.execute(query)

    def exists(self, key: K) -> bool:
        """
        Checks if a row with the given key exists in the table.
        """
        if not isinstance(key, list) and not isinstance(key, tuple):
            key = [key]
        query = QueryGenerator.generate_query_with_pk(self.schema, key)
        result = self.execute_select_query(query).to_item
        return result is not None

    def get_single_element(self, query: QueryBuilder | str) -> Any:
        """
        Returns a single element based on the provided query.
        Raises ValueError if no data is found.
        """
        result = self.execute_select_query(query).to_item
        if not result:
            raise ValueError("No data found")
        return result

    def get_single_element_or_none(self, query: QueryBuilder | str) -> Any | None:
        """
        Returns a single element based on the provided query.
        """
        return self.execute_select_query(query).to_item

    def get_list_of_elements(self, query: QueryBuilder | str) -> list[Any]:
        """
        Returns a list of elements based on the provided query.
        """
        result = self.execute_select_query(query).to_items
        if not result:
            return []
        return result

    def _columns_to_insert(self, data: D) -> list[str]:
        """
        Returns the columns to be inserted into the table.
        """
        columns = self.schema.columns
        auto_generated = data.meta().auto_generated_fields
        for col in self.schema.columns:
            if col in auto_generated and getattr(data, col) is None:
                columns.remove(col)
        return columns

    def update(self, data: D) -> None:
        """
        Updates a row in the table.
        """
        self._update(data)

    def _update(self, data: D) -> None:
        columns = data.meta().db_columns
        schema = self.schema.without_alias()
        pk = self.schema.primaryKey
        column_names = [col for col in schema.columns if col not in pk]
        field_names = [columns[col].db_field_name for col in column_names]
        attr = data.get_values_for_columns(field_names)
        query = (QueryBuilder()
                 .append("UPDATE " + schema.table_name)
                 .append("SET " + ", ".join([f"{col} = ?" for col in column_names]), attr)
                 .append(QueryGenerator.generate_where_with_pk(schema, data.flattened_primary_key)))

        self.execute(query)

    def delete(self, data: D | K = None, key: K = None) -> None:
        """
        Deletes a row from the table.
        """
        assert data or key, "Either data or key must be provided"
        if isinstance(data, self.dataclass) and not key:
            pk = data.flattened_primary_key
        else:
            pk = key
        self._delete(pk)

    def _delete(self, primaryKey: K) -> None:
        schema = self.schema.without_alias()
        query = (QueryBuilder()
                 .append("DELETE FROM " + schema.table_name)
                 .append(QueryGenerator.generate_where_with_pk(schema, primaryKey)))

        self.execute(query)

    def get_data_with_query(self, query: QueryBuilder | str, fetch_type: FetchType = FetchType.EAGER) -> list[D]:
        """
        Returns a list of data objects based on the provided query.
        """
        foreign_key = self.dataclass.meta().foreign_keys.items()
        result = self.execute_select_query(query).to_dict
        if not result:
            return []
        if fetch_type == FetchType.LAZY:
            return [self.dataclass.from_db_dict(row) for row in result]

        for col, values in foreign_key:
            foreign_key_values = {self._get_fk_as_tuple(row, values) for row in result}
            foreign_key_values = list(filter(lambda row: all(item is not None for item in row), foreign_key_values))
            if len(foreign_key_values) == 0:
                continue

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
                        for val in values:
                            row[val.db_field_name] = lookup_map[row_value]
                    else:
                        for val in values:
                            row[val.db_field_name] = None
            else:
                foreign_key_map = {getattr(i, referenced_schema.primaryKey[0]): i for i in foreign_key_result}
                self._update_result_dict(result, values[0].db_field_name, foreign_key_map)

        one_to_many_cols = self.dataclass.meta().one_to_many_fields.items()

        for col, value in one_to_many_cols:
            db_values = {row[value.db_field_name] for row in result}
            query = (QueryBuilder()
                     .append(value.referenced_column).appendIn(list(db_values)))
            one_To_many_result = value.reference().get_data_with_where(query)
            result_map = {}
            for row in one_To_many_result:
                row_value = getattr(row, value.referenced_column)
                if row_value not in result_map:
                    result_map[row_value] = []
                result_map[row_value].append(row)
            self._update_result_dict(result, value.db_field_name, result_map, default=[])

        return [self.dataclass.from_db_dict(row) for row in result]

    def _update_result_dict(self, result: list[dict], column: str, result_map: dict, default: Any = None):
        for row in result:
            row_value = row[column]
            if row_value in result_map:
                row[column] = result_map[row_value]
            else:
                row[column] = default

    def get_one_with_query(self, query: QueryBuilder | str, fetch_type: FetchType = FetchType.EAGER) -> D:
        """
        Returns a single row based on the provided query.
        """
        result = self.get_one_or_none_with_query(query, fetch_type)
        if not result:
            raise ValueError("No data found")
        return result

    def get_one_or_none_with_query(self, query: QueryBuilder | str,
                                   fetch_type: FetchType = FetchType.EAGER) -> D | None:
        """
        Returns a single row based on the provided query.
        """
        result = self.execute_select_query(query).to_dict
        if not result:
            return None
        result_as_dict = result[0]
        if fetch_type == FetchType.EAGER:
            for col, values in self.dataclass.meta().foreign_keys.items():
                key = self._get_fk_as_tuple(result_as_dict, values)
                resolved_value = values[0].reference().get_one(key, nullable=True)
                result_as_dict[col] = resolved_value

            for col, values in self.dataclass.meta().one_to_many_fields.items():
                key = result_as_dict[values.db_field_name]
                query = QueryBuilder().append(f"{values.referenced_column} = ?;", (key,))
                resolved_value = values.reference().get_data_with_where(query)
                result_as_dict[values.db_field_name] = resolved_value or []

        return self.dataclass.from_db_dict(result_as_dict)

    def get_data_with_where(self, query: QueryBuilder | str, fetch_type: FetchType = FetchType.EAGER) -> list[D]:
        """
        Returns a list of data objects based on the provided query where clause.
        """
        query = (QueryBuilder()
                 .append(f"SELECT * FROM {self.schema.table_name_no_alias} WHERE")
                 .append(query))
        return self.get_data_with_query(query, fetch_type)

    def get_one_with_where(self, query: QueryBuilder | str, fetch_type: FetchType = FetchType.EAGER) -> D:
        """
        Returns a single row based on the provided query where clause.
        """
        query = (QueryBuilder()
                 .append(f"SELECT * FROM {self.schema.table_name_no_alias} WHERE")
                 .append(query))
        return self.get_one_with_query(query, fetch_type)

    def get_one_with_join(self, query: QueryBuilder | str, alias: str = None,
                          fetch_type: FetchType = FetchType.EAGER) -> D:
        """
        Returns a single row based on the provided query with join.
        """
        alias = alias or self.schema.alias
        whole_query = QueryBuilder().append(
            f"SELECT {alias}.* FROM {self.schema.table_name_no_alias} AS {alias}").append(query)
        return self.get_one_with_query(whole_query, fetch_type)

    @staticmethod
    def _get_fk_as_tuple(result: dict, fk: list[DBColumn]) -> tuple:
        key = []
        for value in fk:
            key.append(result[value.db_field_name])
        return tuple(key)

    def execute_select_query(self, query: QueryBuilder | str, params: list | None = None) -> DBResult:
        """
        Executes a SELECT query and returns the result.
        """
        if isinstance(query, QueryBuilder):
            params = query.params
            query = query.query
        return super().execute_select_query(query, params)

    def execute(self, query: QueryBuilder | str, params: list | tuple | None = None):
        """
        Executes a query (INSERT, UPDATE, DELETE) and returns the result.
        """
        if isinstance(query, QueryBuilder):
            params = query.params
            query = query.query
        with Connection() as db:
            orm_logger.info("DML-STATEMENT: %s", query)
            db.execute(query, params)
