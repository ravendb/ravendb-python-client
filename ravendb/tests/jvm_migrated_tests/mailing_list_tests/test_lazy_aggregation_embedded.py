from ravendb.documents.indexes.abstract_index_creation_tasks import AbstractIndexCreationTask
from ravendb.tests.test_base import TestBase


class Task:
    def __init__(self, Id: str, assignee_id: str):
        self.Id = Id
        self.assignee_id = assignee_id


class TaskIndex(AbstractIndexCreationTask):
    def __init__(self):
        super(TaskIndex, self).__init__()
        self.map = " from task in docs.Tasks select new { task.assignee_id }"


class TestLazyAggregationEmbedded(TestBase):
    def setUp(self):
        super(TestLazyAggregationEmbedded, self).setUp()

    def test(self):
        with self.store.open_session() as session:
            task1 = Task("tasks/1", "users/1")
            task2 = Task("tasks/2", "users/1")
            task3 = Task("tasks/3", "users/2")
            session.store(task1)
            session.store(task2)
            session.store(task3)
            session.save_changes()

            TaskIndex().execute(self.store)
            self.wait_for_indexing(self.store)

            query = session.query_index_type(TaskIndex, Task).aggregate_by(
                lambda f: f.by_field("assignee_id").with_display_name("assignee_id")
            )
            lazy_operation = query.execute_lazy()
            facet_value = lazy_operation.value

            user_stats = {}
            for value in facet_value.get("assignee_id").values:
                user_stats[value.range_] = value.count_

            self.assertEqual(2, user_stats.get("users/1"))
            self.assertEqual(1, user_stats.get("users/2"))
