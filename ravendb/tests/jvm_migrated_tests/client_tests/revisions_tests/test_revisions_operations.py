from datetime import datetime

from ravendb.documents.operations.revisions import (
    AdoptOrphanedRevisionsOperation,
    ConfigureRevisionsBinCleanerOperation,
    ConfigureRevisionsOperation,
    DeleteRevisionsOperation,
    EnforceRevisionsConfigurationOperation,
    RevertRevisionsByIdOperation,
    RevisionsBinConfiguration,
    RevisionsCollectionConfiguration,
    RevisionsConfiguration,
)
from ravendb.serverwide.operations.revisions import ConfigureRevisionsForConflictsOperation
from ravendb.infrastructure.orders import Company
from ravendb.primitives import constants
from ravendb.tests.test_base import TestBase


class TestRevisionsOperations(TestBase):
    def setUp(self):
        super().setUp()

    def _create_company_with_revisions(self, number_of_updates: int = 4) -> Company:
        company = Company(name="Company Name")
        with self.store.open_session() as session:
            session.store(company)
            session.save_changes()

        for i in range(number_of_updates):
            with self.store.open_session() as session:
                loaded = session.load(company.Id, Company)
                loaded.name = f"Company {i}"
                session.save_changes()

        return company

    def test_enforce_revisions_configuration_purges_excess_revisions(self):
        self.setup_revisions(self.store, False, 100)
        company = self._create_company_with_revisions(4)

        with self.store.open_session() as session:
            self.assertEqual(5, session.advanced.revisions.get_count_for(company.Id))

        # Tighten the configuration - this only affects future modifications...
        configuration = RevisionsConfiguration()
        default_config = RevisionsCollectionConfiguration()
        default_config.minimum_revisions_to_keep = 2
        configuration.default_config = default_config
        self.store.maintenance.send(ConfigureRevisionsOperation(configuration))

        with self.store.open_session() as session:
            self.assertEqual(5, session.advanced.revisions.get_count_for(company.Id))

        # ...until we enforce it on the existing revisions.
        operation = self.store.operations.send_async(EnforceRevisionsConfigurationOperation())
        operation.wait_for_completion()

        with self.store.open_session() as session:
            self.assertEqual(2, session.advanced.revisions.get_count_for(company.Id))

    def test_enforce_revisions_configuration_with_parameters(self):
        self.setup_revisions(self.store, False, 100)
        company = self._create_company_with_revisions(4)

        configuration = RevisionsConfiguration()
        default_config = RevisionsCollectionConfiguration()
        default_config.minimum_revisions_to_keep = 1
        configuration.default_config = default_config
        self.store.maintenance.send(ConfigureRevisionsOperation(configuration))

        parameters = EnforceRevisionsConfigurationOperation.Parameters(
            include_force_created=True, collections=["Companies"]
        )
        operation = self.store.operations.send_async(EnforceRevisionsConfigurationOperation(parameters))
        operation.wait_for_completion()

        with self.store.open_session() as session:
            self.assertEqual(1, session.advanced.revisions.get_count_for(company.Id))

    def test_delete_revisions_by_document_id(self):
        self.setup_revisions(self.store, False, 100)
        company = self._create_company_with_revisions(4)

        with self.store.open_session() as session:
            self.assertEqual(5, session.advanced.revisions.get_count_for(company.Id))

        result = self.store.maintenance.send(DeleteRevisionsOperation(document_id=company.Id))
        self.assertEqual(5, result.total_deletes)

        with self.store.open_session() as session:
            self.assertEqual(0, session.advanced.revisions.get_count_for(company.Id))

    def test_delete_revisions_by_change_vectors(self):
        self.setup_revisions(self.store, False, 100)
        company = self._create_company_with_revisions(4)

        with self.store.open_session() as session:
            metadata = session.advanced.revisions.get_metadata_for(company.Id)
            self.assertEqual(5, len(metadata))
            change_vectors = [m[constants.Documents.Metadata.CHANGE_VECTOR] for m in metadata[:2]]

        result = self.store.maintenance.send(
            DeleteRevisionsOperation(document_id=company.Id, revisions_change_vectors=change_vectors)
        )
        self.assertEqual(2, result.total_deletes)

        with self.store.open_session() as session:
            self.assertEqual(3, session.advanced.revisions.get_count_for(company.Id))

    def test_delete_revisions_validation_is_client_side(self):
        self.assertRaises(ValueError, lambda: DeleteRevisionsOperation(document_ids=[]))
        self.assertRaises(
            ValueError,
            lambda: DeleteRevisionsOperation(
                document_id="companies/1", revisions_change_vectors=["cv"], from_date=datetime(2020, 1, 1)
            ),
        )

    def test_revert_revisions_by_id(self):
        self.setup_revisions(self.store, False, 100)

        company = Company(name="Old Name")
        with self.store.open_session() as session:
            session.store(company)
            session.save_changes()

        with self.store.open_session() as session:
            loaded = session.load(company.Id, Company)
            loaded.name = "New Name"
            session.save_changes()

        with self.store.open_session() as session:
            metadata = session.advanced.revisions.get_metadata_for(company.Id)
            self.assertEqual(2, len(metadata))
            # Metadata is ordered newest-first, so the original ("Old Name") revision is last.
            old_change_vector = metadata[1][constants.Documents.Metadata.CHANGE_VECTOR]

        self.store.operations.send(RevertRevisionsByIdOperation(id_=company.Id, change_vector=old_change_vector))

        with self.store.open_session() as session:
            loaded = session.load(company.Id, Company)
            self.assertEqual("Old Name", loaded.name)

    def test_configure_revisions_bin_cleaner(self):
        configuration = RevisionsBinConfiguration()
        configuration.disabled = False
        configuration.minimum_entries_age_to_keep_in_min = 10
        configuration.cleaner_frequency_in_sec = 100

        result = self.store.maintenance.send(ConfigureRevisionsBinCleanerOperation(configuration))

        self.assertIsNotNone(result)
        self.assertIsNotNone(result.raft_command_index)
        self.assertGreater(result.raft_command_index, 0)

    def test_configure_revisions_for_conflicts(self):
        configuration = RevisionsCollectionConfiguration()
        configuration.minimum_revisions_to_keep = 5

        result = self.store.maintenance.server.send(
            ConfigureRevisionsForConflictsOperation(self.store.database, configuration)
        )

        self.assertIsNotNone(result)
        self.assertIsNotNone(result.raft_command_index)
        self.assertGreater(result.raft_command_index, 0)

    def test_adopt_orphaned_revisions_completes(self):
        self.setup_revisions(self.store, False, 100)

        company = Company(name="Company Name")
        with self.store.open_session() as session:
            session.store(company)
            session.save_changes()

        with self.store.open_session() as session:
            session.delete(company.Id)
            session.save_changes()

        # The operation must run to completion against the server (0 adoptions is a valid result);
        # wait_for_completion raises if the operation faults.
        operation = self.store.operations.send_async(AdoptOrphanedRevisionsOperation())
        operation.wait_for_completion()

    def test_enforce_revisions_configuration_with_max_ops_per_second(self):
        self.setup_revisions(self.store, False, 100)
        company = self._create_company_with_revisions(4)

        configuration = RevisionsConfiguration()
        default_config = RevisionsCollectionConfiguration()
        default_config.minimum_revisions_to_keep = 2
        configuration.default_config = default_config
        self.store.maintenance.send(ConfigureRevisionsOperation(configuration))

        parameters = EnforceRevisionsConfigurationOperation.Parameters(max_ops_per_second=10000)
        operation = self.store.operations.send_async(EnforceRevisionsConfigurationOperation(parameters))
        operation.wait_for_completion()

        with self.store.open_session() as session:
            self.assertEqual(2, session.advanced.revisions.get_count_for(company.Id))

    def test_enforce_parameters_reject_non_positive_max_ops_per_second(self):
        self.assertRaises(ValueError, lambda: EnforceRevisionsConfigurationOperation.Parameters(max_ops_per_second=0))

    def test_delete_revisions_by_date_range(self):
        self.setup_revisions(self.store, False, 100)
        company = self._create_company_with_revisions(4)

        result = self.store.maintenance.send(
            DeleteRevisionsOperation(
                document_id=company.Id, from_date=datetime(2000, 1, 1), to_date=datetime(2100, 1, 1)
            )
        )
        self.assertEqual(5, result.total_deletes)
        with self.store.open_session() as session:
            self.assertEqual(0, session.advanced.revisions.get_count_for(company.Id))

    def test_delete_revisions_for_multiple_documents(self):
        self.setup_revisions(self.store, False, 100)
        first = self._create_company_with_revisions(4)
        second = self._create_company_with_revisions(4)

        result = self.store.maintenance.send(DeleteRevisionsOperation(document_ids=[first.Id, second.Id]))
        self.assertEqual(10, result.total_deletes)
        with self.store.open_session() as session:
            self.assertEqual(0, session.advanced.revisions.get_count_for(first.Id))
            self.assertEqual(0, session.advanced.revisions.get_count_for(second.Id))

    def test_revert_multiple_revisions_by_id(self):
        self.setup_revisions(self.store, False, 100)

        id_to_old_change_vector = {}
        for _ in range(2):
            company = Company(name="Old Name")
            with self.store.open_session() as session:
                session.store(company)
                session.save_changes()
            with self.store.open_session() as session:
                session.load(company.Id, Company).name = "New Name"
                session.save_changes()
            with self.store.open_session() as session:
                metadata = session.advanced.revisions.get_metadata_for(company.Id)
                id_to_old_change_vector[company.Id] = metadata[1][constants.Documents.Metadata.CHANGE_VECTOR]

        self.store.operations.send(RevertRevisionsByIdOperation(id_to_change_vector=id_to_old_change_vector))

        with self.store.open_session() as session:
            for company_id in id_to_old_change_vector:
                self.assertEqual("Old Name", session.load(company_id, Company).name)

    def test_configure_revisions_bin_cleaner_disabled(self):
        result = self.store.maintenance.send(
            ConfigureRevisionsBinCleanerOperation(RevisionsBinConfiguration(disabled=True))
        )
        self.assertIsNotNone(result.raft_command_index)
        self.assertGreater(result.raft_command_index, 0)
