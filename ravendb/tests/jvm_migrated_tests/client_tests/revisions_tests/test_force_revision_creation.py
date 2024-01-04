from ravendb import RevisionsConfiguration, RevisionsCollectionConfiguration
from ravendb.documents.operations.revisions import ConfigureRevisionsOperation
from ravendb.documents.session.misc import ForceRevisionStrategy
from ravendb.infrastructure.orders import Company
from ravendb.tests.test_base import TestBase


class TestForceRevisionCreation(TestBase):
    def setUp(self):
        super().setUp()

    def test_has_revisions_flag_is_created_when_forcing_revision_for_document_that_has_no_revisions_yet(self):
        with self.store.open_session() as session:
            company1 = Company()
            company1.name = "HR1"

            company2 = Company()
            company2.name = "HR2"

            session.store(company1)
            session.store(company2)

            session.save_changes()

            company1id = company1.Id
            company2id = company2.Id

            revisions_count = len(session.advanced.revisions.get_for(company1.Id, Company))
            self.assertEqual(0, revisions_count)

            revisions_count = len(session.advanced.revisions.get_for(company2.Id, Company))
            self.assertEqual(0, revisions_count)

        with self.store.open_session() as session:
            # Force revision with no changes on document
            session.advanced.revisions.force_revision_creation_for_id(company1id)

            # Force revision with changes on document
            session.advanced.revisions.force_revision_creation_for_id(company2id)
            company2 = session.load(company2id, Company)
            company2.name = "HR2 New Name"

            session.save_changes()

        with self.store.open_session() as session:
            revisions = session.advanced.revisions.get_for(company1id, Company)
            revisions_count = len(revisions)
            self.assertEqual(1, revisions_count)
            self.assertEqual("HR1", revisions[0].name)

            revisions = session.advanced.revisions.get_for(company2id, Company)
            revisions_count = len(revisions)
            self.assertEqual(1, revisions_count)
            self.assertEqual("HR2", revisions[0].name)

            # Assert that HasRevisions flag was created on both documents
            company = session.load(company1id, Company)
            metadata = session.advanced.get_metadata_for(company)
            self.assertEqual("HasRevisions", metadata["@flags"])

            company = session.load(company2id, Company)
            metadata = session.advanced.get_metadata_for(company)
            self.assertEqual("HasRevisions", metadata["@flags"])

    def test_force_revision_creation_for_tracked_entity_with_changes_by_entity(self):
        # 1. Store document
        with self.store.open_session() as session:
            company = Company()
            company.name = "HR"
            session.store(company)
            session.save_changes()

            company_id = company.Id

            revisions_count = len(session.advanced.revisions.get_for(company.Id, Company))
            self.assertEqual(0, revisions_count)

        # 2. Load, Make changes & Save
        with self.store.open_session() as session:
            company = session.load(company_id, Company)
            company.name = "HR V2"

            session.advanced.revisions.force_revision_creation_for(company)
            session.save_changes()

            revisions = session.advanced.revisions.get_for(company.Id, Company)
            revisions_count = len(revisions)

            self.assertEqual(1, revisions_count)

            # Assert revision contains the value 'Before' the changes...
            # ('Before' is the default force revision creation strategy)
            self.assertEqual("HR", revisions[0].name)

    def test_force_revision_creation_across_multiple_sessions(self):
        with self.store.open_session() as session:
            company = Company()
            company.name = "HR"

            session.store(company)
            session.save_changes()

            company_id = company.Id
            revisions_count = len(session.advanced.revisions.get_for(company.Id, Company))
            self.assertEqual(0, revisions_count)

            session.advanced.revisions.force_revision_creation_for(company)
            session.save_changes()

            revisions_count = len(session.advanced.revisions.get_for(company.Id, Company))
            self.assertEqual(1, revisions_count)

            # Verify that another 'force' request will not create another revision
            session.advanced.revisions.force_revision_creation_for(company)
            session.save_changes()

            revisions_count = len(session.advanced.revisions.get_for(company.Id, Company))
            self.assertEqual(1, revisions_count)

        with self.store.open_session() as session:
            company = session.load(company_id, Company)
            company.name = "HR V2"

            session.advanced.revisions.force_revision_creation_for(company)
            session.save_changes()

            revisions = session.advanced.revisions.get_for(company.Id, Company)
            revisions_count = len(revisions)

            self.assertEqual(1, revisions_count)
            # Assert revision contains the value 'Before' the changes...
            self.assertEqual("HR", revisions[0].name)

            session.advanced.revisions.force_revision_creation_for(company)
            session.save_changes()

            revisions = session.advanced.revisions.get_for(company.Id, Company)
            revisions_count = len(revisions)

            self.assertEqual(2, revisions_count)

            # Assert revision contains the value 'Before' the changes...
            self.assertEqual("HR V2", revisions[0].name)

        with self.store.open_session() as session:
            company = session.load(company_id, Company)
            company.name = "HR V3"
            session.save_changes()

        with self.store.open_session() as session:
            session.advanced.revisions.force_revision_creation_for_id(company_id)
            session.save_changes()

            revisions = session.advanced.revisions.get_for(company_id, Company)
            revisions_count = len(revisions)

            self.assertEqual(3, revisions_count)
            self.assertEqual("HR V3", revisions[0].name)

    def test_force_revision_creation_multiple_requests(self):
        with self.store.open_session() as session:
            company = Company()
            company.name = "HR"
            session.store(company)
            session.save_changes()

            company_id = company.Id

            revisions_count = len(session.advanced.revisions.get_for(company.Id, Company))
            self.assertEqual(0, revisions_count)

        with self.store.open_session() as session:
            session.advanced.revisions.force_revision_creation_for_id(company_id)

            company = session.load(company_id, Company)
            company.name = "HR V2"

            session.advanced.revisions.force_revision_creation_for(company)
            # The above request should not throw - we ignore duplicate requests with SAME strategy

            self.assertRaisesWithMessageContaining(
                session.advanced.revisions.force_revision_creation_for_id,
                RuntimeError,
                "A request for creating a revision was already made for document",
                company.Id,
                ForceRevisionStrategy.NONE,
            )

            session.save_changes()

            revisions = session.advanced.revisions.get_for(company.Id, Company)
            revisions_count = len(revisions)

            self.assertEqual(1, revisions_count)
            self.assertEqual("HR", revisions[0].name)

    def test_force_revision_creation_for_tracked_entity_with_changes_by_ID(self):
        # 1. Store document
        with self.store.open_session() as session:
            company = Company()
            company.name = "HR"
            session.store(company)
            session.save_changes()

            company_id = company.Id

            revisions_count = len(session.advanced.revisions.get_for(company.Id, Company))
            self.assertEqual(0, revisions_count)

        with self.store.open_session() as session:
            # 2. Load, Make changes & Save
            company = session.load(company_id, Company)
            company.name = "HR V2"

            session.advanced.revisions.force_revision_creation_for(company.Id)
            session.save_changes()

            revisions = session.advanced.revisions.get_for(company.Id, Company)
            revisions_count = len(revisions)

            self.assertEqual(1, revisions_count)

            # Assert revision contains the value 'Before' the changes...
            self.assertEqual("HR", revisions[0].name)

    def test_force_revision_creation_for_single_untracked_entity_by_ID(self):
        with self.store.open_session() as session:
            company = Company()
            company.name = "HR"
            session.store(company)

            company_id = company.Id
            session.save_changes()

        with self.store.open_session() as session:
            session.advanced.revisions.force_revision_creation_for_id(company_id)
            session.save_changes()

            revisions_count = len(session.advanced.revisions.get_for(company_id, Company))
            self.assertEqual(1, revisions_count)

    def test_force_revision_creation_when_revision_configuration_is_set(self):
        # Define revisions settings
        configuration = RevisionsConfiguration()

        companies_configuration = RevisionsCollectionConfiguration()
        companies_configuration.purge_on_delete = True
        companies_configuration.minimum_revisions_to_keep = 5

        configuration.collections = {"Companies": companies_configuration}

        result = self.store.maintenance.send(ConfigureRevisionsOperation(configuration))

        with self.store.open_session() as session:
            company = Company()
            company.name = "HR"
            session.store(company)
            company_id = company.Id
            session.save_changes()

            revisions_count = len(session.advanced.revisions.get_for(company.Id, Company))
            self.assertEqual(1, revisions_count)  # one revision because configuration is set

            session.advanced.revisions.force_revision_creation_for(company)
            session.save_changes()

            revisions_count = len(session.advanced.revisions.get_for(company.Id, Company))
            self.assertEqual(
                1, revisions_count
            )  # no new revision created - already exists due to configuration settings

            session.advanced.revisions.force_revision_creation_for(company)
            session.save_changes()

            company.name = "HR V2"
            session.save_changes()

            revisions = session.advanced.revisions.get_for(company_id, Company)
            revisions_count = len(revisions)

            self.assertEqual(2, revisions_count)
            self.assertEqual("HR V2", revisions[0].name)

        with self.store.open_session() as session:
            session.advanced.revisions.force_revision_creation_for_id(company_id)
            session.save_changes()

            revisions = session.advanced.revisions.get_for(company_id, Company)
            revisions_count = len(revisions)

            self.assertEqual(2, revisions_count)

            self.assertEqual("HR V2", revisions[0].name)

    def test_force_revision_creation_for_new_document_by_entity(self):
        with self.store.open_session() as session:
            company = Company()
            company.name = "HR"
            session.store(company)
            session.save_changes()

            session.advanced.revisions.force_revision_creation_for(company)

            revisions_count = len(session.advanced.revisions.get_for(company.Id, Company))
            self.assertEqual(0, revisions_count)

            session.save_changes()

            revisions_count = len(session.advanced.revisions.get_for(company.Id, Company))
            self.assertEqual(1, revisions_count)

    def test_cannot_force_revision_creation_for_untracked_entity_by_entity(self):
        with self.store.open_session() as session:
            company = Company()
            company.name = "HR"

            self.assertRaisesWithMessage(
                session.advanced.revisions.force_revision_creation_for,
                RuntimeError,
                "Cannot create a revision for the requested entity because it is not tracked by the session",
                company,
            )

    def test_force_revision_creation_for_multiple_untracked_entities_by_ID(self):
        with self.store.open_session() as session:
            company1 = Company()
            company1.name = "HR1"

            company2 = Company()
            company2.name = "HR2"

            session.store(company1)
            session.store(company2)

            companyid1 = company1.Id
            companyid2 = company2.Id

            session.save_changes()

        with self.store.open_session() as session:
            session.advanced.revisions.force_revision_creation_for_id(companyid1)
            session.advanced.revisions.force_revision_creation_for_id(companyid2)

            session.save_changes()

            revisions_count_1 = len(session.advanced.revisions.get_for(companyid1, Company))
            revisions_count_2 = len(session.advanced.revisions.get_for(companyid2, Company))

            self.assertEqual(1, revisions_count_1)
            self.assertEqual(1, revisions_count_2)
