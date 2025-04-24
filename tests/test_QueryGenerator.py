import unittest
from unittest.mock import MagicMock

from pdxorm.AbstractSchema import AbstractSchema
from pdxorm.QueryGenerator import generate_join, generate_where_with_pk


class QueryGeneratorTests(unittest.TestCase):
    def test_generates_where_clause_with_single_pk(self):
        schema = MagicMock(spec=AbstractSchema)
        schema.primaryKey = ["id"]
        schema.alias = "t1"
        pk_values = [1]

        query = generate_where_with_pk(schema, pk_values)

        self.assertEqual(query.query, "WHERE t1.id = ?")
        self.assertEqual(query.params, pk_values)

    def test_generates_where_clause_with_multiple_pk(self):
        schema = MagicMock(spec=AbstractSchema)
        schema.primaryKey = ["id", "name"]
        schema.alias = ""
        pk_values = [1, "test"]

        query = generate_where_with_pk(schema, pk_values)

        self.assertEqual(query.query, "WHERE id = ? AND name = ?")
        self.assertEqual(query.params, pk_values)

    def test_generates_inner_join_clause(self):
        table_schema = MagicMock(spec=AbstractSchema)
        join_schema = MagicMock(spec=AbstractSchema)
        table_schema.alias = "t1"
        join_schema.alias = "t2"
        join_schema.table_name = "join_table"
        join_schema.primaryKey = ["nummer"]
        join_columns = ["id"]

        query = generate_join(table_schema, join_columns, join_schema)

        self.assertEqual(query.query, "INNER JOIN join_table ON t1.id = t2.nummer")

    def test_generates_inner_join_two_values(self):
        table_schema = MagicMock(spec=AbstractSchema)
        join_schema = MagicMock(spec=AbstractSchema)
        table_schema.alias = "t1"
        join_schema.alias = "t2"
        join_schema.table_name = "join_table"
        join_schema.primaryKey = ["id", "name"]
        join_columns = ["id", "sammlung"]

        query = generate_join(table_schema, join_columns, join_schema)

        self.assertEqual(query.query, "INNER JOIN join_table ON t1.id = t2.id AND t1.sammlung = t2.name")

    def test_generates_left_join_clause(self):
        table_schema = MagicMock(spec=AbstractSchema)
        join_schema = MagicMock(spec=AbstractSchema)
        table_schema.alias = "t1"
        join_schema.alias = "t2"
        join_schema.table_name = "join_table"
        join_schema.primaryKey = ["id"]
        join_columns = ["id"]

        query = generate_join(table_schema, join_columns, join_schema, join_type="LEFT")

        self.assertEqual(query.query, "LEFT JOIN join_table ON t1.id = t2.id")

    def test_handles_empty_primary_key_in_where_clause(self):
        schema = MagicMock(spec=AbstractSchema)
        schema.primaryKey = []
        pk_values = []

        query = generate_where_with_pk(schema, pk_values)

        self.assertEqual(query.query, "WHERE ")
        self.assertEqual(query.params, [])
