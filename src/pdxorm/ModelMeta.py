from dataclasses import dataclass

from pdxorm.DBColumn import DBColumn


@dataclass
class MetaInformation:
    fields: dict[str, DBColumn]
    db_columns: dict[str, DBColumn]
    primary_keys: list[DBColumn]
    foreign_keys: dict[str, list[DBColumn]]

    auto_generated_fields: list[DBColumn]


class ModelMeta(type):
    def __new__(mcs, name, bases, cls_dict):  # noqa: ANN001

        # skip Meta class for the base model
        if name == 'BaseModel':
            return super().__new__(mcs, name, bases, cls_dict)

        fields: dict[str, DBColumn | list[DBColumn]] = {}
        db_columns: dict[str, DBColumn] = {}
        primary_key_field: list[DBColumn] = []
        foreign_key_field: dict[str, list[DBColumn]] = {}
        auto_generated_field: list[DBColumn] = []

        # delete the passed class attributes values
        for key, value in list(cls_dict.items()):
            if isinstance(value, DBColumn):
                value.model_attribute = key  # tell the field which attribute it belongs to
                if value.field_name is None:  # default db name
                    value.field_name = key
                fields[key] = value  # save the field in the dict
                db_columns[value.db_field_name] = value  # save the field in the db_columns dict
                if value.primary_key:
                    primary_key_field.append(value)
                if value.auto_generated:
                    auto_generated_field.append(value.field_name)
                if value.reference:
                    foreign_key_field[key] = [value]

                del cls_dict[key]
            elif isinstance(value, list):
                # check if the list contains DBColumn instances
                if all(isinstance(item, DBColumn) for item in value):
                    for item in value:
                        db_columns[item.db_field_name] = item
                        if item.primary_key:
                            primary_key_field.append(item)
                    foreign_key_field[key] = value
                    fields[key] = value

        if not primary_key_field:
            pass
            # logging.warning(f"[Meta Warning] Model '{name}' does not have a primary key defined.")

        # save the collected data in the meta dict
        meta = {
            'fields': fields,  # Dict from {model_attr: Field_instance}
            'primary_keys': primary_key_field,  # List of primary key fields
            'db_columns': db_columns,  # Map from {db_column_name: Field_instance}
            'auto_generated_fields': auto_generated_field,
            'foreign_keys': foreign_key_field,  # Map from {model_attr: [Field_instance]}
        }
        meta = MetaInformation(**meta)
        cls_dict['_meta'] = meta  # save the meta dict in the class dict

        # create the new class
        new_class = super().__new__(mcs, name, bases, cls_dict)

        return new_class
