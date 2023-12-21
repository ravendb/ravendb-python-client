from ravendb.documents.operations.statistics import GetStatisticsOperation
from ravendb.tests.test_base import TestBase, Company


class TestRavenDB_11058(TestBase):
    def setUp(self):
        super(TestRavenDB_11058, self).setUp()

    def test_can_copy_attachment(self):
        with self.store.open_session() as session:
            company = Company(name="HR")

            session.store(company, "companies/1")

            bais1 = bytes([1, 2, 3])
            session.advanced.attachments.store(company, "file1", bais1)

            bais2 = bytes([3, 2, 1])
            session.advanced.attachments.store(company, "file10", bais2)

            session.save_changes()

        stats = self.store.maintenance.send(GetStatisticsOperation())
        self.assertEqual(2, stats.count_of_attachments)
        self.assertEqual(2, stats.count_of_unique_attachments)

        with self.store.open_session() as session:
            new_company = Company(name="CF")
            session.store(new_company, "companies/2")

            old_company = session.load("companies/1", Company)
            session.advanced.attachments.copy(old_company, "file1", new_company, "file2")
            session.save_changes()

        stats = self.store.maintenance.send(GetStatisticsOperation())
        self.assertEqual(3, stats.count_of_attachments)
        self.assertEqual(2, stats.count_of_unique_attachments)

        with self.store.open_session() as session:
            self.assertTrue(session.advanced.attachments.exists("companies/1", "file1"))
            self.assertFalse(session.advanced.attachments.exists("companies/1", "file2"))
            self.assertTrue(session.advanced.attachments.exists("companies/1", "file10"))

            self.assertFalse(session.advanced.attachments.exists("companies/2", "file1"))
            self.assertTrue(session.advanced.attachments.exists("companies/2", "file2"))
            self.assertFalse(session.advanced.attachments.exists("companies/2", "file10"))

    def test_can_rename_attachment(self):
        with self.store.open_session() as session:
            company = Company(name="HR")
            session.store(company, "companies/1-A")

            bais1 = bytes([1, 2, 3])
            session.advanced.attachments.store(company, "file1", bais1)

            bais2 = bytes([3, 2, 1])
            session.advanced.attachments.store(company, "file10", bais2)

            session.save_changes()

        stats = self.store.maintenance.send(GetStatisticsOperation())
        self.assertEqual(2, stats.count_of_attachments)
        self.assertEqual(2, stats.count_of_unique_attachments)

        with self.store.open_session() as session:
            company = session.load("companies/1-A", Company)
            session.advanced.attachments.rename(company, "file1", "file2")
            session.save_changes()

        stats = self.store.maintenance.send(GetStatisticsOperation())
        self.assertEqual(2, stats.count_of_attachments)
        self.assertEqual(2, stats.count_of_unique_attachments)

        with self.store.open_session() as session:
            self.assertFalse(session.advanced.attachments.exists("companies/1-A", "file1"))
            self.assertTrue(session.advanced.attachments.exists("companies/1-A", "file2"))

        with self.store.open_session() as session:
            company = session.load("companies/1-A", Company)
            session.advanced.attachments.rename(company, "file2", "file3")
            session.save_changes()

        stats = self.store.maintenance.send(GetStatisticsOperation())
        self.assertEqual(2, stats.count_of_attachments)
        self.assertEqual(2, stats.count_of_unique_attachments)

        with self.store.open_session() as session:
            self.assertFalse(session.advanced.attachments.exists("companies/1-A", "file2"))
            self.assertTrue(session.advanced.attachments.exists("companies/1-A", "file3"))

        with self.store.open_session() as session:
            company = session.load("companies/1-A", Company)
            session.advanced.attachments.rename(company, "file3", "file10")
            with self.assertRaises(Exception):
                # todo: replace with ConcurrencyException after implementing request executor error handling
                session.save_changes()

        stats = self.store.maintenance.send(GetStatisticsOperation())
        self.assertEqual(2, stats.count_of_attachments)
        self.assertEqual(2, stats.count_of_unique_attachments)

    def test_can_move_attachment(self):
        with self.store.open_session() as session:
            company = Company(name="HR")
            session.store(company, "companies/1")

            bais1 = bytes([1, 2, 3])
            session.advanced.attachments.store(company, "file1", bais1)
            bais2 = bytes([3, 2, 1])
            session.advanced.attachments.store(company, "file10", bais2)
            bais3 = bytes([4, 5, 6])
            session.advanced.attachments.store(company, "file20", bais3)

            session.save_changes()

        stats = self.store.maintenance.send(GetStatisticsOperation())
        self.assertEqual(3, stats.count_of_attachments)
        self.assertEqual(3, stats.count_of_unique_attachments)

        with self.store.open_session() as session:
            new_company = Company(name="CF")
            session.store(new_company, "companies/2")

            old_company = session.load("companies/1", Company)

            session.advanced.attachments.move(old_company, "file1", new_company, "file2")
            session.save_changes()

        self.assertEqual(3, stats.count_of_attachments)
        self.assertEqual(3, stats.count_of_unique_attachments)

        with self.store.open_session() as session:
            self.assertFalse(session.advanced.attachments.exists("companies/1", "file1"))
            self.assertFalse(session.advanced.attachments.exists("companies/1", "file2"))
            self.assertTrue(session.advanced.attachments.exists("companies/1", "file10"))
            self.assertTrue(session.advanced.attachments.exists("companies/1", "file20"))

            self.assertFalse(session.advanced.attachments.exists("companies/2", "file1"))
            self.assertTrue(session.advanced.attachments.exists("companies/2", "file2"))
            self.assertFalse(session.advanced.attachments.exists("companies/2", "file10"))
            self.assertFalse(session.advanced.attachments.exists("companies/2", "file20"))

        with self.store.open_session() as session:
            session.advanced.attachments.move("companies/1", "file10", "companies/2", "file3")
            session.save_changes()

        self.assertEqual(3, stats.count_of_attachments)
        self.assertEqual(3, stats.count_of_unique_attachments)

        with self.store.open_session() as session:
            self.assertFalse(session.advanced.attachments.exists("companies/1", "file1"))
            self.assertFalse(session.advanced.attachments.exists("companies/1", "file2"))
            self.assertFalse(session.advanced.attachments.exists("companies/1", "file3"))
            self.assertFalse(session.advanced.attachments.exists("companies/1", "file0"))
            self.assertTrue(session.advanced.attachments.exists("companies/1", "file20"))

            self.assertFalse(session.advanced.attachments.exists("companies/2", "file1"))
            self.assertTrue(session.advanced.attachments.exists("companies/2", "file2"))
            self.assertTrue(session.advanced.attachments.exists("companies/2", "file3"))
            self.assertFalse(session.advanced.attachments.exists("companies/2", "file10"))
            self.assertFalse(session.advanced.attachments.exists("companies/2", "file20"))

        with self.store.open_session() as session:
            session.advanced.attachments.move("companies/1", "file20", "companies/2", "file3")  # should throw

            with self.assertRaises(Exception):
                # todo: replace with ConcurrencyException after implementing request executor error handling
                session.save_changes()
