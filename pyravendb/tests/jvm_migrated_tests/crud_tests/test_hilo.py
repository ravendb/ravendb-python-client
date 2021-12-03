from concurrent.futures import ThreadPoolExecutor

from pyravendb.documents import DocumentStore
from pyravendb.documents.identity import HiLoIdGenerator, MultiDatabaseHiLoGenerator
from pyravendb.tests.test_base import TestBase, User, UserWithId


class HiLoDocument:
    def __init__(self, maximum):
        self.Max = maximum
        # Uppercase required - Server responds on NextHiLoCommand using Max document field


class Product:
    def __init__(self, name=None):
        self.product_name = name


class TestHiLo(TestBase):
    def setUp(self):
        super(TestHiLo, self).setUp()

    def test_hilo_can_not_go_down(self):
        with self.store.open_session() as session:
            hilo_doc = HiLoDocument(32)
            session.store(hilo_doc, "Raven/HiLo/users")
            session.save_changes()

            hi_lo_key_gen = HiLoIdGenerator(
                "users", self.store, self.store.database, self.store.conventions.identity_parts_separator
            )
            ids = [hi_lo_key_gen.next_id()]

            hilo_doc.Max = 12
            session.store(hilo_doc, "Raven/Hilo/users", None)
            session.save_changes()

            for i in range(128):
                next_id = hi_lo_key_gen.next_id()
                self.assertNotIn(next_id, ids)
                ids.append(next_id)

            self.assertEqual(len(set(ids)), len(ids))

    def test_hilo_multi_db(self):
        with self.store.open_session() as session:
            hilo_doc = HiLoDocument(64)
            session.store(hilo_doc, "Raven/Hilo/users")

            products_hilo = HiLoDocument(128)
            session.store(products_hilo, "Raven/Hilo/products")

            session.save_changes()

            multi_db_hilo = MultiDatabaseHiLoGenerator(self.store)
            generate_document_key = multi_db_hilo.generate_document_id(None, User())
            self.assertEqual(generate_document_key, "users/65-A")
            generate_document_key = multi_db_hilo.generate_document_id(None, Product())
            self.assertEqual(generate_document_key, "products/129-A")

    def test_capacity_should_double(self):
        hilo_generator = HiLoIdGenerator(
            "users", self.store, self.store.database, self.store.conventions.identity_parts_separator
        )
        with self.store.open_session() as session:
            hilo_doc = HiLoDocument(64)
            session.store(hilo_doc, "Raven/Hilo/users")
            session.save_changes()

            for i in range(32):
                hilo_generator.generate_document_id(User())

        with self.store.open_session() as session:
            hilo_doc = session.load("Raven/Hilo/users", HiLoDocument)
            self.assertEqual(hilo_doc.Max, 96)
            hilo_generator.generate_document_id(User())

        with self.store.open_session() as session:
            hilo_doc = session.load("Raven/Hilo/users", HiLoDocument)
            self.assertEqual(hilo_doc.Max, 160)

    def test_return_unused_range_on_close(self):
        new_store = DocumentStore(self.store.urls, self.store.database)
        new_store.initialize()
        with new_store.open_session() as session:
            hilo_doc = HiLoDocument(32)
            session.store(hilo_doc, "Raven/Hilo/users")
            session.save_changes()

            session.store(User())
            session.store(User())
            session.save_changes()

        new_store.close()

        new_store = DocumentStore(self.store.urls, self.store.database)
        new_store.initialize()

        with new_store.open_session() as session:
            hilo_doc = session.load("Raven/Hilo/users", HiLoDocument)
            self.assertEqual(34, hilo_doc.Max)

        new_store.close()

    def test_does_not_get_another_range_when_doing_parallel_requests(self):
        parallel_level = 32
        users = [UserWithId(None, x) for x in range(parallel_level)]

        def future_store_user(i):
            def store_user():
                user = users[i]
                session = self.store.open_session()
                session.store(user)
                session.save_changes()

            return store_user

        tasks = [future_store_user(i) for i in range(32)]

        with ThreadPoolExecutor(max_workers=32) as executor:
            for task in tasks:
                executor.submit(task)
            executor.shutdown()

        for user in users:
            key_number = user.Id.split("/")[1].split("-")[0]
            self.assertLess(int(key_number), 33)
