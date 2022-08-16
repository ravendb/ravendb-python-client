import datetime

from ravendb import AbstractIndexCreationTask
from ravendb.documents.indexes.definitions import FieldStorage
from ravendb.tests.test_base import TestBase


class MainDocument:
    def __init__(self, name: str, Id: str):
        self.name = name
        self.Id = Id


class RelatedDocument:
    def __init__(self, name: str, value: float, Id: str):
        self.name = name
        self.value = value
        self.Id = Id


class MyIndex(AbstractIndexCreationTask):
    def __init__(self):
        super().__init__()
        self.map = (
            "docs.MainDocuments.Select(mainDocument => new {"
            "    mainDocument = mainDocument,"
            '    related = this.LoadDocument(String.Format("related/{0}", mainDocument.name), "RelatedDocuments")'
            "}).Select(this0 => new {"
            "    name = this0.mainDocument.name,\n"
            "    value = this0.related != null ? ((decimal ? ) this0.related.value) : ((decimal ? ) null)\n"
            "})"
        )
        self._store_all_fields(FieldStorage.YES)

    class Result:
        def __init__(self, name: str, value: float):
            self.name = name
            self.value = value


class TestRavenDB16334(TestBase):
    def setUp(self):
        super(TestRavenDB16334, self).setUp()

    def _can_wait_for_indexes_with_load_after_save_changes_internal(self, all_indexes: bool) -> None:
        MyIndex().execute(self.store)

        with self.store.open_session() as session:
            main_document = MainDocument("A", "main/A")
            session.store(main_document)

            related_document = RelatedDocument("A", 21.5, "related/A")
            session.store(related_document)

            session.save_changes()

        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            result = session.query_index_type(MyIndex, MyIndex.Result).select_fields(MyIndex.Result).single()
            self.assertEqual("21.5", result.value)  # todo: check why it isn't parsed to int and change the str value

        # act
        with self.store.open_session() as session:
            session.advanced.wait_for_indexes_after_save_changes(
                lambda builder: builder.with_timeout(datetime.timedelta(seconds=15))
                .throw_on_timeout(True)
                .wait_for_indexes(None if all_indexes else ["MyIndex"])
            )

            related = session.load("related/A", RelatedDocument)
            related.value = 42
            session.save_changes()

        # assert
        with self.store.open_session() as session:
            result = session.query_index_type(MyIndex, MyIndex.Result).select_fields(MyIndex.Result).single()

            self.assertEqual("42", result.value)  # todo: check why it isn't parsed to int and change the str value

    def test_can_wait_for_indexes_with_load_after_save_changes_all_indexes(self):
        self._can_wait_for_indexes_with_load_after_save_changes_internal(True)

    def test_can_wait_for_indexes_with_load_after_save_changes_single_index(self):
        self._can_wait_for_indexes_with_load_after_save_changes_internal(False)
