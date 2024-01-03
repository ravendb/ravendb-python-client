from time import sleep

from ravendb.infrastructure.entities import User
from ravendb.primitives import constants
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
