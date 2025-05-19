import unittest
from unittest.mock import MagicMock

from pdxorm import AbstractTable
from pdxorm.BaseData import BaseData
from pdxorm.DBColumn import DBColumn

testDb = MagicMock(spec=AbstractTable)


class TestData(BaseData):
    x: int = DBColumn("x", "x", False, None, primary_key=True)
    y: str = DBColumn("y", "tralalero", False, None)
    z: str = DBColumn("z", "tralala", False, None, default_value="hallo")


class ForeignKeyData(BaseData):
    id: int = DBColumn("id", "id", False, None, primary_key=True)
    name: str = DBColumn("name", "name", False, None)


class newTestData(BaseData):
    id: int = DBColumn("id", "id", False, None, primary_key=True)
    name: str = DBColumn("name", "name", False, None)
    foreign_key: ForeignKeyData = DBColumn("foreign_key", "foreign_key", False, testDb)


testDb.dataclass = ForeignKeyData


class BaseDataTest(unittest.TestCase):
    def setUp(self):
        self.test_data = TestData(x=1, y="test")
        self.test_data2 = TestData(x=2, y="test2", z="sdfsd")
        self.test_data3 = TestData(x=2, y="test2", z="hallo")
        self.test_with_fk = newTestData(id=1, name="tum tum tum", foreign_key={"id": 1, "name": "test"})


    def test_equal(self):
        self.assertEqual(self.test_data, TestData(x=1, y="test", z="hallo"))
        self.assertNotEqual(self.test_data, self.test_data2)
        self.assertNotEqual(self.test_data, None)

    def test_repr(self):
        self.assertEqual(repr(self.test_data), "TestData(x=1, y=test, z=hallo)")

    def test_json(self):
        self.assertEqual(
            self.test_data.as_json(indent=0).replace("\n", ""),
            '{"x": 1,"y": "test","z": "hallo"}'
        )
        self.assertEqual(
            self.test_data2.as_json(indent=0).replace("\n", ""),
            '{"x": 2,"y": "test2","z": "sdfsd"}'
        )

    def test_json_with_fk(self):
        self.assertEqual(
            self.test_with_fk.as_json(indent=0).replace("\n", ""),
            '{"id": 1,"name": "tum tum tum","foreign_key": {"id": 1,"name": "test"}}'
        )

    def test_load_from_json(self):
        import json
        data = TestData(**json.loads(self.test_data.as_json()))
        self.assertEqual(data.x, 1)
        self.assertEqual(data.y, "test")
        self.assertEqual(data.z, "hallo")

        data2 = newTestData(**json.loads(self.test_with_fk.as_json()))
        self.assertEqual(data2.id, 1)
        self.assertEqual(data2.name, "tum tum tum")
        self.assertEqual(data2.foreign_key.id, 1)
        self.assertEqual(data2.foreign_key.name, "test")

    def test_call_with_dict(self):
        test_data = TestData(**{"x": 1, "y": "test", "z": "hallo"})
        self.assertEqual(test_data.x, 1)
        self.assertEqual(test_data.y, "test")
        self.assertEqual(test_data.z, "hallo")


    def test_call_with_dict_as_foreign_key(self):
        self.assertEqual(self.test_with_fk.id, 1)
        self.assertEqual(self.test_with_fk.name, "tum tum tum")
        self.assertEqual(self.test_with_fk.foreign_key.id, 1)
        self.assertEqual(self.test_with_fk.foreign_key.name, "test")

    def test_json_handles_nested_foreign_keys(self):
        nested_fk = newTestData(
            id=2,
            name="nested test",
            foreign_key={"id": 3, "name": "nested foreign key"}
        )
        self.assertEqual(
            nested_fk.as_json(indent=0).replace("\n", ""),
            '{"id": 2,"name": "nested test","foreign_key": {"id": 3,"name": "nested foreign key"}}'
        )

    def test_flattened_primary_key_handles_nested_keys(self):
        nested_fk = newTestData(
            id=2,
            name="nested test",
            foreign_key={"id": 3, "name": "nested foreign key"}
        )
        self.assertEqual(
            nested_fk.flattened_primary_key,
            (2,)
        )

    def test_from_db_dict_converts_correctly(self):
        db_dict = {"id": 1, "name": "test", "foreign_key": ForeignKeyData(**{"id": 2, "name": "fk"})}
        obj = newTestData.from_db_dict(db_dict)
        self.assertEqual(obj.id, 1)
        self.assertEqual(obj.name, "test")
        self.assertEqual(obj.foreign_key.id, 2)
        self.assertEqual(obj.foreign_key.name, "fk")


    def test_from_db_dict_handles_null_values(self):
        db_dict = {"x": 1, "tralalero": "hallo", "tralala": "moin moin"}
        obj = TestData.from_db_dict(db_dict)
        self.assertEqual(obj.x, 1)
        self.assertEqual(obj.y, "hallo")
        self.assertEqual(obj.z, "moin moin")

    def test_get_as_db_name(self):
        self.assertEqual(self.test_data.get_as_db_name("x"), 1)
        self.assertEqual(self.test_data.get_as_db_name("tralalero"), "test")
        self.assertEqual(self.test_data.get_as_db_name("tralala"), "hallo")
        with self.assertRaises(ValueError):
            self.test_data.get_as_db_name("non_existing_column")

    def test_get_values_for_columns(self):
        columns = ["x", "tralalero"]
        values = self.test_data.get_values_for_columns(columns)
        self.assertEqual(values, [1, "test"])

        columns = ["id", "foreign_key"]
        values = self.test_with_fk.get_values_for_columns(columns)
        self.assertEqual(values, [1, 1])

        with self.assertRaises(ValueError):
            columns = ["non_existing_column"]
            values = self.test_data.get_values_for_columns(columns)
