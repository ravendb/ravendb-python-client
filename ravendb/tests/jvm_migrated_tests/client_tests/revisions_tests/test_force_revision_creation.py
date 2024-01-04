from ravendb.infrastructure.orders import Company
from ravendb.tests.test_base import TestBase


class TestForceRevisionCreation(TestBase):
    def setUp(self):
        super().setUp()

    def test_has_revisions_flag_is_created_when_forcing_revision_for_document_that_has_no_revisions_yet(self):
        company1id = ""
        company2id = ""

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
