import json
import typing
from typing import Any

from .ModelMeta import MetaInformation, ModelMeta
from .utils import get_as_tuple, get_first_or_element


class LazyField:
    def __init__(self, db_value: Any, reference: Any):
        self.db_value = db_value
        self.reference = reference


class BaseData[K: tuple](metaclass=ModelMeta):
    def __init__(self, **kwargs):
        self._meta: MetaInformation = self.__class__._meta  # Zugriff auf die Metadaten der Klasse
        self._data = kwargs.get("date_from_db_raw", None)  # Raw-Data from db if given
        self._loaded_from_db = False  # Flag, ob das Objekt aus der DB kommt

        for field_name, field_obj in self._meta.fields.items():
            field_obj = get_first_or_element(field_obj)
            # Setze Standardwerte oder übergebene Werte
            if isinstance(field_obj, list):
                default_value = None
            else:
                default_value = field_obj.default_value
            value = kwargs.get(field_name, default_value)
            if isinstance(value, dict):
                # Wenn der Wert ein Dictionary ist, konvertiere ihn in das richtige Format
                value = field_obj.reference.dataclass(**value)
            if isinstance(value, list):
                # Wenn der Wert eine Liste ist, konvertiere ihn in die richtige Form
                value = [field_obj.reference.dataclass(**item) if isinstance(item, dict) else item for item in value]

            if value is not None and not isinstance(value, LazyField) and get_first_or_element(
                    field_obj).reference and not self._is_type_or_list_type(value, BaseData):
                value = LazyField(value, get_first_or_element(field_obj).reference)
            setattr(self, field_name, value)

        self.validate_types()

    def __repr__(self):
        field_values = ', '.join(f"{k}={getattr(self, k)}" for k in self._meta.fields.keys())
        return f"{self.__class__.__name__}({field_values})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return False
        if self.pk != other.pk:
            return False
        for field_name in self._meta.fields.keys():
            if self.get_db_value(field_name) != other.get_db_value(field_name):
                return False
        return True

    def __getattribute__(self, name: str) -> Any:
        value = object.__getattribute__(self, name)
        if isinstance(value, LazyField):
            # calculate LAZY fetch
            raise AttributeError
        return value

    @classmethod
    def from_db_dict(cls, db_dict: dict) -> "BaseData":
        """
        Converts a dictionary from the database to an instance of the class.
        No dict nesting allowed only other instances of BaseData
        """
        new_dict = {}
        for field_name, field_obj in cls.meta().fields.items():
            if isinstance(field_obj, list):
                value = db_dict.get(field_obj[0].db_field_name, None)
                reference = field_obj[0].reference
            else:
                value = db_dict.get(field_obj.db_field_name, None)
                reference = field_obj.reference

            if reference and value is not None and not cls._is_type_or_list_type(value, reference.dataclass):
                new_dict[field_name] = LazyField(value, reference)
            else:
                new_dict[field_name] = value
        return cls(**new_dict, date_from_db_raw=db_dict)

    @staticmethod
    def _is_type_or_list_type(value: Any, expected_type: Any) -> bool:
        """
        Check if the value is of the expected type or a list of the expected type.
        """
        if isinstance(value, list):
            return all(isinstance(item, expected_type) for item in value)
        return isinstance(value, expected_type)

    def validate_types(self):
        """
        Validates the types of the attributes of the object. (AI)
        """
        for name, expected_type in self.__annotations__.items():
            origin = typing.get_origin(expected_type)
            args = typing.get_args(expected_type)
            value = self.__dict__[name]
            if isinstance(value, LazyField):
                continue
            if value is None:
                continue

            if origin is typing.Union and type(None) in args:  # Optional types
                non_none_args = tuple(arg for arg in args if arg is not type(None))
                isvalid = isinstance(value, non_none_args) or value is None
            elif origin is typing.Literal: # Literal types
                isvalid = value in args
            elif origin:  # Other generic types
                isvalid = isinstance(value, origin)
            else:  # Simple types
                isvalid_boolean_int = expected_type is bool and isinstance(value, int) and value in (0, 1)  # Allow int to be used as boolean
                isvalid = isinstance(value, expected_type) or isvalid_boolean_int

            if not isvalid:
                raise TypeError(f"Type of {name} is not {expected_type}, but {type(value)}: {value}")

    @property
    def primary_key(self) -> tuple:
        """
        Returns the primary key of the object.
        """
        return tuple(getattr(self, field.field_name) for field in self._meta.primary_keys)

    @property
    def pk(self) -> K:
        """
        Returns the primary key of the object as a database representation.
        """
        key = []

        for field in self._meta.primary_keys:
            key.extend(self.get_db_value(field.field_name))
        return tuple(key)

    @classmethod
    def meta(cls) -> MetaInformation:
        """
        Returns the meta information of the object.
        """
        return cls._meta

    def get_as_db_name(self, db_colum: str) -> Any:
        """
        Arguments:
            db_colum: The database column name to get the value for.
        Returns the value of the object as a database name.
        """
        if db_colum not in self._meta.db_columns:
            raise ValueError(f"Column {db_colum} not found in meta information")
        return getattr(self, self._meta.db_columns[db_colum].field_name)

    def get_db_value(self, attribute: str) -> tuple[Any, ...]:
        """
        Arguments:
            attribute: The attribute name to get the value for.
        Returns the value of the object as a database value.
        """
        if attribute not in self._meta.fields:
            raise ValueError(f"Column {attribute} not found in meta information")
        value = self.__dict__[attribute]
        if value is None:
            return (None,) * len(get_as_tuple(self._meta.fields[attribute]))
        if isinstance(value, LazyField):
            return (value.db_value,)
        if isinstance(value, BaseData):
            return value.pk
        if isinstance(value, list):
            return tuple([x.pk if isinstance(x, BaseData) else x for x in value])
        return (value,)

    def set_db_value(self, attribute: str, value: Any) -> None:
        """
        Sets the value of the object for the given attribute.
        Args:
            attribute: The attribute name to set the value for.
            value: The value to set.
        """
        if attribute not in self._meta.fields:
            raise ValueError(f"Column {attribute} not found in meta information")
        if isinstance(value, LazyField):
            value = value.db_value
        if isinstance(value, BaseData) or self._meta.db_columns[attribute].reference:
            value = LazyField(value, self._meta.db_columns[attribute].reference)
        self.__dict__[attribute] = value

    def get_values_for_columns(self, columns: list[str] | set[str]) -> list[Any]:
        """
        Returns the values for the given columns.
        Args:
            columns: A list of database column names to get values for.
        """
        values: list[Any] = []
        already_seen: set[Any] = set()
        for col in columns:
            if col not in self._meta.db_columns:
                raise ValueError(f"Column {col} not found in meta information")
            attr_name = self.meta().db_columns[col].field_name
            if attr_name in already_seen:
                continue
            already_seen.add(attr_name)
            values.extend(self.get_db_value(attr_name))

        return values

    def as_json(self, indent: int = 2, default: Any = str) -> str:
        """
        Returns a JSON representation of the object.
        """
        return json.dumps(self.as_dict(), indent=indent, default=default)

    def as_dict(self) -> dict:
        """
        Returns a JSON representation of the object as a dictionary.
        """
        return {k: self._dict_or_elem(getattr(self, k)) for k in self._meta.fields.keys()}

    def _dict_or_elem(self, obj: Any):  # noqa: ANN202
        if isinstance(obj, list):
            return [self._dict_or_elem(x) for x in obj]
        elif isinstance(obj, BaseData):
            return obj.as_dict()
        elif isinstance(obj, dict):
            return {k: self._dict_or_elem(v) for k, v in obj.items()}
        else:
            return obj

    def copy(self) -> typing.Self:
        """
        Returns a copy of the object.
        """
        return self.__class__(**self.as_dict(), date_from_db_raw=self._data)
