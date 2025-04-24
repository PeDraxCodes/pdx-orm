from .AbstractSchema import AbstractSchema
from .QueryBuilder import QueryBuilder


def generate_where_with_pk(schema: AbstractSchema, pk: list) -> QueryBuilder:
    """
    Generates a WHERE clause based on the primary key of the schema.
    """

    query = QueryBuilder()
    alias = ""
    if schema.alias:
        alias = schema.alias + "."

    query.append("WHERE " + " AND ".join([f"{alias}{col} = ?" for col in schema.primaryKey]), pk)
    return query


def generate_query_with_pk(schema: AbstractSchema, key: list) -> QueryBuilder:
    return (
        QueryBuilder()
        .append(f"{schema.select} FROM " + schema.table_name)
        .append(generate_where_with_pk(schema, key))
    )


def generate_join(
        table_schema: AbstractSchema,
        join_columns: list[str],
        join_schema: AbstractSchema,
        join_type: str = "INNER"
) -> QueryBuilder:
    """
    Generates a JOIN clause for the given schema and join schema.
    """
    query = QueryBuilder()
    query.append(
        f"{join_type} JOIN {join_schema.table_name} ON " + " AND ".join(
            [f"{table_schema.alias}.{col2} = {join_schema.alias}.{col}" for col, col2 in
             zip(join_schema.primaryKey, join_columns)])
    )
    return query
