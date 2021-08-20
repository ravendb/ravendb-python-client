from pyravendb.tests.test_base import TestBase
from pyravendb.store.document_session import _RefEq
from dataclasses import dataclass


@dataclass()
class TestData:
    entry: str


class TestDocumentsByEntity(TestBase):
    def setUp(self):
        super(TestDocumentsByEntity, self).setUp()

    def test_ref_eq(self):
        data = TestData(entry="classified")

        wrapped_data_1 = _RefEq(data)
        wrapped_data_2 = _RefEq(wrapped_data_1)

        self.assertTrue(wrapped_data_2 == wrapped_data_1)
        with self.assertRaises(TypeError):
            wrapped_data_1 == data
