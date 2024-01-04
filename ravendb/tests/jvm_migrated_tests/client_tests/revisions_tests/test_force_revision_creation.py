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
