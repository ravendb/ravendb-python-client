from ravendb import IndexQuery, DeleteByQueryOperation
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase


class TestDeleteByQuery(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_delete_by_query(self):
        with self.store.open_session() as session:
            session.store(User(age=5))
            session.store(User(age=10))
            session.save_changes()

        index_query = IndexQuery("from users where age == 5")
        operation = DeleteByQueryOperation(index_query)
        async_op = self.store.operations.send_async(operation)

        async_op.wait_for_completion()

        with self.store.open_session() as session:
            self.assertEqual(1, session.query(object_type=User).count())
