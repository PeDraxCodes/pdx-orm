import json
import typing
from typing import Any

from db.ORM.ModelMeta import MetaInformation, ModelMeta


class BaseData(metaclass=ModelMeta):
    def __init__(self, **kwargs):
        self._meta: MetaInformation = self.__class__._meta  # Zugriff auf die Metadaten der Klasse
        self._data = {}  # Hier könnten die aktuellen Werte der Instanz gespeichert werden
        self._loaded_from_db = False  # Flag, ob das Objekt aus der DB kommt

        for field_name, field_obj in self._meta.fields.items():
            # Setze Standardwerte oder übergebene Werte
            if isinstance(field_obj, list):
                default_value = None
            else:
                default_value = field_obj.default_value
            value = kwargs.get(field_name, default_value)
            if isinstance(value, dict):
                # Wenn der Wert ein Dictionary ist, konvertiere ihn in das richtige Format
                value = field_obj.reference.data(**value)
            setattr(self, field_name, value)

        self.validate_types()

    def __repr__(self):
        # Gibt eine lesbare Darstellung des Objekts zurück
        field_values = ', '.join(f"{k}={getattr(self, k)}" for k in self._meta.fields.keys())
        return f"{self.__class__.__name__}({field_values})"

    def __eq__(self, other: object) -> bool:
        # Vergleicht zwei Objekte basierend auf den Werten der Felder
        if not isinstance(other, self.__class__):
            return False
        for field_name in self._meta.fields.keys():
            if getattr(self, field_name) != getattr(other, field_name):
                return False
        return True

    def validate_types(self):
        # check types referenced in the annotations

        for name, expected_type in self.__annotations__.items():
            origin = typing.get_origin(expected_type)
            args = typing.get_args(expected_type)
            value = getattr(self, name)

            if origin is typing.Literal:
                continue
            if not all(not isinstance(value, list) for arg in args):
                continue

            if origin is typing.Union and type(None) in args:  # Optional-Typ
                non_none_args = tuple(arg for arg in args if arg is not type(None))
                isvalid = isinstance(value, non_none_args) or value is None
            elif origin:  # Andere generische Typen
                isvalid = isinstance(value, origin)
            else:  # Standardtypen
                isvalid = isinstance(value, expected_type) or not (
                        expected_type is type(bool) and isinstance(value, int))

            if not isvalid:
                raise TypeError(f"Type of {name} is not {expected_type}, but {type(value)}")

    @property
    def primary_key(self) -> list:
        """
        Returns the primary key of the object.
        """
        return [getattr(self, field.field_name) for field in self._meta.primary_keys]

    @property
    def flattened_primary_key(self) -> tuple:
        """
        Returns the primary key of the object as a flattened list.
        """
        key = []
        for k in self.primary_key:
            if isinstance(k, BaseData):
                key.extend(k.flattened_primary_key)
            else:
                key.append(k)

        return tuple(key)

    @classmethod
    def meta(cls) -> MetaInformation:
        """
        Returns the meta information of the object.
        """
        return cls._meta

    def get_as_db_name(self, db_colum: str) -> Any:
        """
        Returns the value of the object as a database name.
        """
        if db_colum not in self._meta.db_columns:
            raise ValueError(f"Column {db_colum} not found in meta information")
        return getattr(self, self._meta.db_columns[db_colum].field_name)

    def get_values_for_columns(self, columns: list[str]) -> list[Any]:
        """
        Returns the values for the given columns.
        """
        values: list[Any] = []
        for field in self._meta.fields.values():
            if isinstance(field, list):
                value = getattr(self, field[0].field_name)
                if value is None:
                    values.extend([None] * len(field))
                    continue
                value = value.flattened_primary_key
                if not all(x.db_field_name in columns for x in field):
                    continue
                values.extend(value)
            else:
                value = getattr(self, field.field_name)
                if field.db_field_name in columns:
                    values.append(value)
        return values

    def json(self) -> str:
        """
        Returns a JSON representation of the object.
        """
        return json.dumps(self.json_as_dict())

    def json_as_dict(self) -> dict:
        """
        Returns a JSON representation of the object as a dictionary.
        """
        return {k: getattr(self, k) for k in self._meta.fields.keys()}
