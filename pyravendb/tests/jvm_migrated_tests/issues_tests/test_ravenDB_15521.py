from pyravendb.tests.test_base import TestBase


class SimpleDoc:
    def __init__(self, Id=None, name=None):
        self.Id = Id
        self.name = name


class TestRavenDB15521(TestBase):
    def setUp(self):
        super(TestRavenDB15521, self).setUp()

    def test_should_work(self):
        with self.store.open_session() as session:
            doc = SimpleDoc("TestDoc", "State1")
            session.store(doc)

            attachment = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            session.advanced.attachment.store(doc, "TestAttachment", bytes(attachment, "utf-8"))

            session.save_changes()

            change_vector_1 = session.get_change_vector_for(doc)
            session.advanced.refresh(doc)
            change_vector_2 = session.get_change_vector_for(doc)
            self.assertEqual(change_vector_1, change_vector_2)
