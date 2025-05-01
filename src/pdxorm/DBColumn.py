from typing import Any, Optional, Type

from pdxorm import AbstractTable


class DBColumn:
    """
    InfoField is a class that represents a field in a database table.

    Attributes:
        field_name (str): The name of the field.
        nullable (bool): Whether the field can be null or not.
        reference (Optional[AbstractTable]): The reference to the table that the field is a foreign key to
        primary_key (bool): Whether the field is a primary key or not.
        default_value: The default value of the field.
    """

    def __init__(self, field_name: str, db_field_name: str, nullable: bool, reference: Optional[Type["AbstractTable"]],
                 primary_key: bool = False,
                 default_value: Optional[Any] = None, auto_generated: bool = False,
                 referenced_column: str | None = None) -> None:
        self.field_name = field_name
        self.db_field_name = db_field_name
        self.nullable = nullable
        self.reference = reference
        self.primary_key = primary_key
        self.default_value = default_value
        self.auto_generated = auto_generated
        self.referenced_column = referenced_column

    def __repr__(self) -> str:
        return f"DBColumn(field_name={self.field_name}, db_field_name={self.db_field_name}, nullable={self.nullable}, " \
               f"reference={self.reference}, primary_key={self.primary_key}, default_value={self.default_value})"
