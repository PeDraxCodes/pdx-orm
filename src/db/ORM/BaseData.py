from db.ORM.ModelMeta import ModelMeta


class BaseData(metaclass=ModelMeta):
    def __init__(self, **kwargs):
        self._meta: dict = self.__class__._meta  # Zugriff auf die Metadaten der Klasse
        self._data = {}  # Hier könnten die aktuellen Werte der Instanz gespeichert werden
        self._loaded_from_db = False  # Flag, ob das Objekt aus der DB kommt

        for field_name, field_obj in self._meta['fields'].items():
            # Setze Standardwerte oder übergebene Werte
            value = kwargs.get(field_name, field_obj.default_value)
            setattr(self, field_name, value)

    def __repr__(self):
        # Gibt eine lesbare Darstellung des Objekts zurück
        field_values = ', '.join(f"{k}={getattr(self, k)}" for k in self._meta['fields'].keys())
        return f"{self.__class__.__name__}({field_values})"

    def __eq__(self, other: object) -> bool:
        # Vergleicht zwei Objekte basierend auf den Werten der Felder
        if not isinstance(other, self.__class__):
            return False
        for field_name in self._meta['fields'].keys():
            if getattr(self, field_name) != getattr(other, field_name):
                return False
        return True

    @property
    def primary_key(self) -> list:
        """
        Returns the primary key of the object.
        """
        return [getattr(self, field.field_name) for field in self._meta['primary_key_field']]

    @property
    def meta(self) -> dict:
        """
        Returns the meta information of the object.
        """
        return self._meta
