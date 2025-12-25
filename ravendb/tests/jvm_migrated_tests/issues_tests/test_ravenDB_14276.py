from ravendb.documents.session.event_args import BeforeStoreEventArgs

from ravendb.tests.test_base import TestBase, User


class TestRavenDB14276(TestBase):
    def setUp(self):
        super(TestRavenDB14276, self).setUp()
        self.dictionary = {"123": {"aaaa": 1}, "321": {"bbbb": 2}}

    @staticmethod
    def on_before_store(event_args: BeforeStoreEventArgs):
        event_args.document_metadata["Some-MetadataEntry"] = "Updated"

    def verify_data(self, doc_id: str):
        with self.store.open_session() as session:
            user = session.load(doc_id, object_type=User)
            self.assertEqual("Updated document", user.name)

            metadata = session.advanced.get_metadata_for(user)
            dictionary = metadata["Custom-Metadata"]

            nested_dictionary = dictionary["123"]
            self.assertEqual(1, nested_dictionary["aaaa"])

            nested_dictionary = dictionary["321"]
            self.assertEqual(2, nested_dictionary["bbbb"])


    def test_can_update_metadata_with_nested_dictionary(self):
        doc_id = "users/1"

        with self.store.open_session() as session:
            session.add_before_store(self.on_before_store)

            user = User("Some document")
            session.store(user, doc_id)

            metadata = session.advanced.get_metadata_for(user)
            metadata["Custom-Metadata"] = self.dictionary
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load(doc_id, object_type=User)
            user.name = "Updated document"
            session.save_changes()

        self.verify_data(doc_id)

    def test_can_update_metadata_with_nested_dictionary_same_session(self):
        doc_id = "users/1"

        with self.store.open_session() as session:
            session.add_before_store(self.on_before_store)

            saved_user = User("Some document")
            session.store(saved_user, doc_id)

            metadata = session.advanced.get_metadata_for(saved_user)
            metadata["Custom-Metadata"] = self.dictionary
            session.save_changes()

            user = session.load(doc_id, object_type=User)
            user.name = "Updated document"
            session.save_changes()

        self.verify_data(doc_id)
