from pyravendb.documents.operations.compare_exchange.operations import PutCompareExchangeValueOperation
from pyravendb.documents.session.misc import CmpXchg
from pyravendb.infrastructure.entities import User
from pyravendb.tests.test_base import TestBase


class TestQueriesWithCustomFunctions(TestBase):
    def test_query_cmp_xchg_where(self):
        self.store.operations.send(PutCompareExchangeValueOperation("Tom", "Jerry", 0))
        self.store.operations.send(PutCompareExchangeValueOperation("Hera", "Zeus", 0))
        self.store.operations.send(PutCompareExchangeValueOperation("Gaya", "Uranus", 0))
        self.store.operations.send(PutCompareExchangeValueOperation("Jerry@gmail.com", "users/2", 0))
        self.store.operations.send(PutCompareExchangeValueOperation("Zeus@gmail.com", "users/1", 0))

        with self.store.open_session() as session:
            jerry = User()
            jerry.name = "Jerry"
            session.store(jerry, "users/2")
            session.save_changes()

            zeus = User()
            zeus.name = "Zeus"
            zeus.last_name = "Jerry"
            session.store(zeus, "users/1")
            session.save_changes()

        with self.store.open_session() as session:
            q = (
                session.advanced.document_query(object_type=User)
                .where_equals("name", CmpXchg.value("Hera"))
                .where_equals("last_name", CmpXchg.value("Tom"))
            )

            self.assertEqual("from 'Users' where name = cmpxchg($p0) and last_name = cmpxchg($p1)", q.index_query.query)

            query_result = list(q)
            self.assertEqual(1, len(query_result))
            self.assertEqual("Zeus", query_result[0].name)

            user = list(
                session.advanced.document_query(object_type=User).where_not_equals("name", CmpXchg.value("Hera"))
            )
            self.assertEqual(1, len(user))

            self.assertEqual("Jerry", user[0].name)

            users = list(session.advanced.raw_query('from Users where name = cmpxchg("Hera")', User))
            self.assertEqual(1, len(users))
            self.assertEqual("Zeus", users[0].name)
