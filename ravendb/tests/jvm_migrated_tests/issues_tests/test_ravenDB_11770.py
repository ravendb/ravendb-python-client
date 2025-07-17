import datetime
import time

from ravendb.tests.test_base import TestBase, Company


class TestRavenDB11770(TestBase):
    def setUp(self):
        super(TestRavenDB11770, self).setUp()

    def test_can_get_revisions_by_date(self):
        print(self.store.identifier.split("(")[0])

        with self.store.open_session() as session:
            id = "users/1"

            self.setup_revisions(self.store, False, 1000)

            company = Company(name="Fitzchak")
            session.store(company, id)
            session.save_changes()

        time.sleep(2)

        fst = datetime.datetime.now(datetime.UTC)

        for i in range(3):
            with self.store.open_session() as session:
                user = session.load(id, object_type=Company)
                user.name = f"Fitzchak {i}"
                session.save_changes()

            time.sleep(2)

        snd = datetime.datetime.now(datetime.UTC)

        for i in range(3):
            with self.store.open_session() as session:
                user = session.load(id, object_type=Company)
                user.name = f"Oren {i}"
                session.save_changes()

            time.sleep(2)

        with self.store.open_session() as session:
            rev1 = session.advanced.revisions.get_by_before_date(id, fst, object_type=Company)
            self.assertEqual("Fitzchak", rev1.name)

            rev2 = session.advanced.revisions.get_by_before_date(id, snd, object_type=Company)
            self.assertEqual("Fitzchak 2", rev2.name)

            rev3 = session.advanced.revisions.get_by_before_date(id, datetime.datetime.now(datetime.UTC), object_type=Company)
            self.assertEqual("Oren 2", rev3.name)
