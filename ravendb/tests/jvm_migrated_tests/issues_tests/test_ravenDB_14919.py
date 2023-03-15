from typing import List, Optional

from ravendb.documents.commands.crud import GetDocumentsCommand
from ravendb.documents.operations.counters import GetCountersOperation
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase


class TestRavenDB14919(TestBase):
    def test_get_documents_command_should_discard_null_ids_post_get(self):
        with self.store.open_session() as session:
            ids = []
            for i in range(1000):
                id = f"users/{i}"
                ids.append(id)
                session.store(User(), id)
            session.save_changes()

        ids.extend([None for _ in range(24)])

        re = self.store.get_request_executor()
        command = GetDocumentsCommand.from_multiple_ids(ids, metadata_only=False)
        re.execute_command(command)

        self.assertEqual(1001, len(command.result.results))
        command.result.results.sort(key=lambda x: x is None)
        self.assertIsNone(command.result.results[len(command.result.results) - 1])

    def test_get_documents_command_should_discard_null_ids(self):
        with self.store.open_session() as session:
            ids = []
            for i in range(100):
                id = f"users/{i}"
                ids.append(id)
                session.store(User(), id)
            session.save_changes()

        ids.extend([None for _ in range(24)])

        re = self.store.get_request_executor()
        command = GetDocumentsCommand.from_multiple_ids(ids, metadata_only=False)
        re.execute_command(command)

        self.assertEqual(101, len(command.result.results))
        command.result.results.sort(key=lambda x: x is None)
        self.assertIsNone(command.result.results[len(command.result.results) - 1])

    def test_get_counter_operations_should_discard_null_counters(self):
        key = "users/2"

        counter_names: List[Optional[str]] = [None] * 124
        with self.store.open_session() as session:
            session.store(User(), key)

            c = session.counters_for(key)
            for i in range(100):
                name = f"likes{i}"
                counter_names.append(name)
                c.increment(name)

            session.save_changes()

        vals = self.store.operations.send(GetCountersOperation(key, counter_names))
        self.assertEqual(101, len(vals.counters))
        vals.counters = sorted(vals.counters, key=lambda x: x is not None, reverse=True)
        for counter in vals.counters[0:100]:
            self.assertEqual(1, counter.total_value)

        self.assertIsNone(vals.counters[len(vals.counters) - 1])

        # test with return_full_results = True
        vals = self.store.operations.send(GetCountersOperation(key, counter_names, True))

        self.assertEqual(101, len(vals.counters))
        vals.counters = sorted(vals.counters, key=lambda x: x is not None, reverse=True)
        for counter in vals.counters[0:100]:
            self.assertEqual(1, len(counter.counter_values))

        self.assertIsNone(vals.counters[len(vals.counters) - 1])
