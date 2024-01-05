from time import sleep

from ravendb import RevisionsConfiguration, RevisionsCollectionConfiguration
from ravendb.documents.commands.revisions import GetRevisionsBinEntryCommand
from ravendb.documents.operations.revisions import ConfigureRevisionsOperation
from ravendb.infrastructure.entities import User
from ravendb.infrastructure.orders import Company
from ravendb.primitives import constants
from ravendb.primitives.constants import int_max
from ravendb.tests.test_base import TestBase


class TestRevisions(TestBase):
    def setUp(self):
        super().setUp()

    def test_revisions(self):
        TestBase.setup_revisions(self.store, False, 4)

        for i in range(4):
            with self.store.open_session() as session:
                user = User(name=f"user{i+1}")
                session.store(user, "users/1")
                session.save_changes()

        sleep(2)
        with self.store.open_session() as session:
            all_revisions = session.advanced.revisions.get_for("users/1", User)
            self.assertEqual(4, len(all_revisions))
            self.assertEqual(["user4", "user3", "user2", "user1"], [x.name for x in all_revisions])

            sleep(2)
            revisions_skip_first = session.advanced.revisions.get_for("users/1", User, 1)
            self.assertEqual(3, len(revisions_skip_first))
            self.assertEqual(["user3", "user2", "user1"], [x.name for x in revisions_skip_first])

            sleep(2)
            revisions_skip_first_take_two = session.advanced.revisions.get_for("users/1", User, 1, 2)
            self.assertEqual(2, len(revisions_skip_first_take_two))
            self.assertEqual(["user3", "user2"], [x.name for x in revisions_skip_first_take_two])

            sleep(2)
            all_metadata = session.advanced.revisions.get_metadata_for("users/1")
            self.assertEqual(4, len(all_metadata))

            sleep(2)
            metadata_skip_first = session.advanced.revisions.get_metadata_for("users/1", 1)
            self.assertEqual(3, len(metadata_skip_first))

            sleep(2)
            metadata_skip_first_take_two = session.advanced.revisions.get_metadata_for("users/1", 1, 2)
            self.assertEqual(2, len(metadata_skip_first_take_two))

            sleep(2)

            user = session.advanced.revisions.get_by_change_vector(
                metadata_skip_first[0].metadata.get(constants.Documents.Metadata.CHANGE_VECTOR), User
            )
            self.assertEqual("user3", user.name)

    def test_can_get_revisions_by_change_vector(self):
        id_ = "users/1"
        self.setup_revisions(self.store, False, 100)

        with self.store.open_session() as session:
            user = User()
            user.name = "Fitzchak"
            session.store(user, id_)
            session.save_changes()

        for i in range(10):
            with self.store.open_session() as session:
                user = session.load(id_, Company)
                user.name = f"Fitzchak{i}"
                session.save_changes()

        with self.store.open_session() as session:
            revisions_metadata = session.advanced.revisions.get_metadata_for(id_)
            self.assertEqual(11, len(revisions_metadata))

            change_vectors = [x[constants.Documents.Metadata.CHANGE_VECTOR] for x in revisions_metadata]
            change_vectors.append("NotExistsChangeVector")

            revisions = session.advanced.revisions.get_by_change_vectors(change_vectors, User)
            self.assertIsNone(revisions.get("NotExistsChangeVector"))
            self.assertIsNone(session.advanced.revisions.get_by_change_vector("NotExistsChangeVector", User))

    def test_can_get_revisions_count_for(self):
        company = Company()
        company.name = "Company Name"

        self.setup_revisions(self.store, False, 100)
        with self.store.open_session() as session:
            session.store(company)
            session.save_changes()

        with self.store.open_session() as session:
            company2 = session.load(company.Id, Company)
            company2.fax = "Israel"
            session.save_changes()

        with self.store.open_session() as session:
            company3 = session.load(company.Id, Company)
            company3.name = "Hibernating Rhinos"
            session.save_changes()

        with self.store.open_session() as session:
            companies_revisions_count = session.advanced.revisions.get_count_for(company.Id)
            self.assertEqual(3, companies_revisions_count)

    def test_can_list_revisions_bin(self):
        self.setup_revisions(self.store, False, 4)

        with self.store.open_session() as session:
            user = User(name="user1")
            session.store(user, "users/1")
            session.save_changes()

        with self.store.open_session() as session:
            session.delete("users/1")
            session.save_changes()

        revisions_bin_entry_command = GetRevisionsBinEntryCommand(0, 20)
        self.store.get_request_executor().execute_command(revisions_bin_entry_command)

        result = revisions_bin_entry_command.result
        self.assertEqual(1, len(result.results))

        self.assertEqual("users/1", result.results[0].get("@metadata").get("@id"))

    def test_collection_case_sensitive_test_1(self):
        id_ = "user/1"
        configuration = RevisionsConfiguration()

        collection_configuration = RevisionsCollectionConfiguration()
        collection_configuration.disabled = False

        configuration.collections = {"uSErs": collection_configuration}

        self.store.maintenance.send(ConfigureRevisionsOperation(configuration))

        with self.store.open_session() as session:
            user = User(name="raven")
            session.store(user, id_)
            session.save_changes()

        for i in range(10):
            with self.store.open_session() as session:
                user = session.load(id_, Company)
                user.name = "raven" + str(i)
                session.save_changes()

        with self.store.open_session() as session:
            revisions_metadata = session.advanced.revisions.get_metadata_for(id_)
            self.assertEqual(11, len(revisions_metadata))

    def test_collection_case_sensitive_test_2(self):
        id_ = "uSEr/1"
        configuration = RevisionsConfiguration()

        collection_configuration = RevisionsCollectionConfiguration()
        collection_configuration.disabled = False

        configuration.collections = {"users": collection_configuration}
        self.store.maintenance.send(ConfigureRevisionsOperation(configuration))

        with self.store.open_session() as session:
            user = User(name="raven")
            session.store(user, id_)
            session.save_changes()

        for i in range(10):
            with self.store.open_session() as session:
                user = session.load(id_, Company)
                user.name = "raven" + str(i)
                session.save_changes()

        with self.store.open_session() as session:
            revisions_metadata = session.advanced.revisions.get_metadata_for(id_)
            self.assertEqual(11, len(revisions_metadata))

    def test_collection_case_sensitive_test_3(self):
        configuration = RevisionsConfiguration()
        c1 = RevisionsCollectionConfiguration()
        c1.disabled = False

        c2 = RevisionsCollectionConfiguration()
        c2.disabled = False

        configuration.collections = {"users": c1, "USERS": c2}
        with self.assertRaises(RuntimeError):
            self.store.maintenance.send(ConfigureRevisionsOperation(configuration))
