import logging

from db.InfoField import InfoField


class ModelMeta(type):
    def __new__(mcs, name, bases, cls_dict):  # noqa: ANN001

        # skip Meta class for the base model
        if name == 'BaseModel':
            return super().__new__(mcs, name, bases, cls_dict)

        fields: dict[str, InfoField] = {}
        primary_key_field: list[InfoField] = []

        # delete the passed class attributes values
        for key, value in list(cls_dict.items()):
            if isinstance(value, InfoField):
                value.model_attribute = key  # tell the field which attribute it belongs to
                if value.field_name is None:  # default db name
                    value.field_name = key
                fields[key] = value  # save the field in the dict
                if value.primary_key:
                    primary_key_field.append(value)
                del cls_dict[key]

        if not primary_key_field:
            logging.warning(f"[Meta Warning] Model '{name}' does not have a primary key defined.")

        # save the collected data in the meta dict
        meta = {
            'fields': fields,  # Dict from {model_attr: Field_instance}
            'primary_key_field': primary_key_field,  # List of primary key fields
            'db_columns': {f.field_name: f for f in fields.values()}  # Map from {db_column_name: Field_instance}
        }
        cls_dict['_meta'] = meta  # save the meta dict in the class dict

        # create the new class
        new_class = super().__new__(mcs, name, bases, cls_dict)

        return new_class
