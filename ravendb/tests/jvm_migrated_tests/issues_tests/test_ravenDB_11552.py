from ravendb import PatchRequest
from ravendb.documents.commands.batches import (
    PutAttachmentCommandData,
    DeleteAttachmentCommandData,
    CopyAttachmentCommandData,
    MoveAttachmentCommandData,
    DeleteCommandData,
    PatchCommandData,
)
from ravendb.tests.test_base import TestBase, Company


class TestRavenDB11552(TestBase):
    def setUp(self):
        super(TestRavenDB11552, self).setUp()

    def test_attachment_put_and_delete_will_work(self):
        with self.store.open_session() as session:
            company = Company(name="HR")

            file0stream = bytes([1, 2, 3])
            session.store(company, "companies/1")
            session.advanced.attachments.store(company, "file0", file0stream)
            session.save_changes()

            self.assertEqual(1, len(session.advanced.attachments.get_names(company)))

        with self.store.open_session() as session:
            company = session.load("companies/1", Company)

            self.assertIsNotNone(company)
            self.assertTrue(session.is_loaded("companies/1"))
            self.assertEqual(1, session.number_of_requests)
            self.assertEqual(1, len(session.advanced.attachments.get_names(company)))

            file1stream = bytes([1, 2, 3])
            session.defer(PutAttachmentCommandData("companies/1", "file1", file1stream, None, None))
            session.save_changes()

            self.assertTrue(session.is_loaded("companies/1"))
            self.assertEqual(2, session.number_of_requests)
            self.assertEqual(2, len(session.advanced.attachments.get_names(company)))

            session.defer(DeleteAttachmentCommandData("companies/1", "file1", None))
            session.save_changes()

            self.assertTrue(session.is_loaded("companies/1"))
            self.assertEqual(3, session.number_of_requests)
            self.assertEqual(1, len(session.advanced.attachments.get_names(company)))

    def test_attachment_copy_and_move_will_work(self):
        with self.store.open_session() as session:
            company1 = Company(name="HR")
            company2 = Company(name="HR")

            session.store(company1, "companies/1")
            session.store(company2, "companies/2")

            file1stream = bytes([1, 2, 3])
            session.advanced.attachments.store(company1, "file1", file1stream)
            session.save_changes()

            self.assertEqual(1, len(session.advanced.attachments.get_names(company1)))

        with self.store.open_session() as session:
            company1 = session.load("companies/1", Company)
            company2 = session.load("companies/2", Company)

            self.assertIsNotNone(company1)
            self.assertTrue(session.is_loaded("companies/1"))
            self.assertEqual(2, session.number_of_requests)
            self.assertEqual(1, len(session.advanced.attachments.get_names(company1)))

            self.assertIsNotNone(company2)
            self.assertTrue(session.is_loaded("companies/2"))
            self.assertEqual(2, session.number_of_requests)
            self.assertEqual(0, len(session.advanced.attachments.get_names(company2)))

            session.defer(CopyAttachmentCommandData("companies/1", "file1", "companies/2", "file1", None))
            session.save_changes()

            self.assertTrue(session.is_loaded("companies/1"))
            self.assertEqual(3, session.number_of_requests)
            self.assertEqual(1, len(session.advanced.attachments.get_names(company1)))

            self.assertTrue(session.is_loaded("companies/2"))
            self.assertEqual(3, session.number_of_requests)
            self.assertEqual(1, len(session.advanced.attachments.get_names(company2)))

            session.defer(MoveAttachmentCommandData("companies/1", "file1", "companies/2", "file2", None))
            session.save_changes()

            self.assertTrue(session.is_loaded("companies/1"))
            self.assertEqual(4, session.number_of_requests)
            self.assertEqual(0, len(session.advanced.attachments.get_names(company1)))

            self.assertTrue(session.is_loaded("companies/2"))
            self.assertEqual(4, session.number_of_requests)
            self.assertEqual(2, len(session.advanced.attachments.get_names(company2)))

    def test_delete_will_work(self):
        with self.store.open_session() as session:
            company = Company(name="HR")
            session.store(company, "companies/1")
            session.save_changes()

        with self.store.open_session() as session:
            company = session.load("companies/1", Company)
            self.assertIsNotNone(company)
            self.assertTrue(session.is_loaded("companies/1"))
            self.assertEqual(1, session.advanced.number_of_requests)

            session.defer(DeleteCommandData("companies/1", None))
            session.save_changes()

            self.assertFalse(session.is_loaded("companies/1"))
            self.assertEqual(2, session.advanced.number_of_requests)

            company = session.load("companies/1", Company)
            self.assertIsNone(company)
            self.assertTrue(session.is_loaded("companies/1"))
            self.assertEqual(3, session.advanced.number_of_requests)

    def test_patch_will_work(self):
        with self.store.open_session() as session:
            company = Company(name="HR")
            session.store(company, "companies/1")
            session.save_changes()

        with self.store.open_session() as session:
            company = session.load("companies/1", Company)
            self.assertIsNotNone(company)
            self.assertTrue(session.is_loaded("companies/1"))
            self.assertEqual(1, session.number_of_requests)

            patch_request = PatchRequest()
            patch_request.script = "this.name = 'HR2';"
            session.defer(PatchCommandData("companies/1", None, patch_request, None))
            session.save_changes()

            self.assertTrue(session.is_loaded("companies/1"))
            self.assertEqual(2, session.advanced.number_of_requests)

            company2 = session.load("companies/1", Company)
            self.assertIsNotNone(company2)
            self.assertTrue(session.is_loaded("companies/1"))
            self.assertEqual(2, session.advanced.number_of_requests)
            self.assertIs(company2, company)
            self.assertEqual("HR2", company2.name)

    def test_patch_will_update_tracked_document_after_save_changes(self):
        with self.store.open_session() as session:
            company = Company(name="HR")
            session.store(company, "companies/1")
            session.save_changes()

        with self.store.open_session() as session:
            company = session.load("companies/1", Company)
            session.advanced.patch(company, "name", "CF")

            cv = session.get_change_vector_for(company)
            last_modified = session.get_last_modified_for(company)

            session.save_changes()

            self.assertEqual("CF", company.name)

            self.assertNotEqual(cv, session.get_change_vector_for(company))
            self.assertNotEqual(last_modified, session.get_last_modified_for(company))

            company.phone = 123
            session.save_changes()

        with self.store.open_session() as session:
            company = session.load("companies/1", Company)
            self.assertEqual("CF", company.name)
            self.assertEqual(123, company.phone)
