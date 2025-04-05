from db.ORM.AbstractSchema import AbstractSchema
from db.ORM.QueryBuilder import QueryBuilder


def generate_where_with_pk(schema: AbstractSchema) -> QueryBuilder:
    """
    Generates a WHERE clause based on the primary key of the schema.
    """

    query = QueryBuilder()
    query.append("WHERE " + " AND ".join([f"{col} = ?" for col in schema.primaryKey]))
    return query
