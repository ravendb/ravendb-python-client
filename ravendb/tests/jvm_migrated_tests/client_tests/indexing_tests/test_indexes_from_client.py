import time
from typing import Optional

from ravendb.documents.session.query import QueryStatistics
from ravendb.documents.queries.index_query import IndexQuery
from ravendb.documents.commands.explain import ExplainQueryCommand
from ravendb.documents.operations.statistics import GetStatisticsOperation
from ravendb.documents.indexes.index_creation import IndexCreation
from ravendb.documents.queries.more_like_this import MoreLikeThisOptions
from ravendb.documents.operations.indexes import (
    GetIndexNamesOperation,
    DeleteIndexOperation,
    ResetIndexOperation,
    GetIndexingStatusOperation,
    StopIndexingOperation,
    StartIndexingOperation,
    StopIndexOperation,
    GetIndexesOperation,
    GetIndexStatisticsOperation,
    SetIndexesLockOperation,
    SetIndexesPriorityOperation,
    GetTermsOperation,
)
from ravendb.documents.indexes.definitions import (
    FieldIndexing,
    FieldStorage,
    IndexLockMode,
    IndexRunningStatus,
    IndexPriority,
)
from ravendb.documents.indexes.abstract_index_creation_tasks import AbstractIndexCreationTask
from ravendb.infrastructure.entities import User, Post
from ravendb.tests.test_base import TestBase


class Users_ByName(AbstractIndexCreationTask):
    def __init__(self):
        super().__init__()
        self.map = "from u in docs.Users select new { u.name }"
        self._index("name", FieldIndexing.SEARCH)

        self._index_suggestions.add("name")

        self._store("name", FieldStorage.YES)


class Posts_ByTitleAndDesc(AbstractIndexCreationTask):
    def __init__(self):
        super(Posts_ByTitleAndDesc, self).__init__()
        self.map = "from p in docs.Posts select new { p.title, p.desc }"
        self._index("title", FieldIndexing.SEARCH)
        self._store("title", FieldStorage.YES)
        self._analyze("title", "Lucene.Net.Analysis.SimpleAnalyzer")

        self._index("desc", FieldIndexing.SEARCH)
        self._store("desc", FieldStorage.YES)
        self._analyze("desc", "Lucene.Net.Analysis.SimpleAnalyzer")


class UsersIndex(AbstractIndexCreationTask):
    def __init__(self):
        super().__init__()
        self.map = "from user in docs.users select new { user.name }"


class TestIndexesFromClient(TestBase):
    def setUp(self):
        super(TestIndexesFromClient, self).setUp()

    def test_can_create_indexes_using_index_creation(self):
        IndexCreation.create_indexes([Users_ByName()], self.store)

        with self.store.open_session() as session:
            user1 = User("users/1", "Marcin")
            session.store(user1)
            session.save_changes()

        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            users = list(session.query_index_type(Users_ByName, User))
            self.assertEqual(1, len(users))

    def test_more_like_this(self):
        with self.store.open_session() as session:
            post1 = Post("posts/1", "doduck", "prototype")
            post2 = Post("posts/2", "doduck", "prototype your idea")
            post3 = Post("posts/3", "doduck", "love programming")
            post4 = Post("posts/4", "We do", "prototype")
            post5 = Post("posts/5", "We love", "challange")

            session.store(post1)
            session.store(post2)
            session.store(post3)
            session.store(post4)
            session.store(post5)
            session.save_changes()

        Posts_ByTitleAndDesc().execute(self.store)
        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            options = MoreLikeThisOptions(minimum_document_frequency=1, minimum_term_frequency=0)
            results = list(
                session.query_index_type(Posts_ByTitleAndDesc, Post).more_like_this(
                    lambda f: f.using_document(lambda x: x.where_equals("id()", "posts/1")).with_options(options)
                )
            )
            self.assertEqual(3, len(results))

            self.assertEqual("doduck", results[0].title)
            self.assertEqual("prototype your idea", results[0].desc)
            self.assertEqual("doduck", results[1].title)
            self.assertEqual("love programming", results[1].desc)
            self.assertEqual("We do", results[2].title)
            self.assertEqual("prototype", results[2].desc)

    def test_can_execute_many_indexes(self):
        self.store.execute_indexes([UsersIndex()])
        index_names_operation = GetIndexNamesOperation(0, 10)
        index_names = self.store.maintenance.send(index_names_operation)
        self.assertEqual(1, len(index_names))

    def test_can_delete(self):
        self.store.execute_index(UsersIndex())
        self.store.maintenance.send(DeleteIndexOperation(UsersIndex().index_name))

        command = GetStatisticsOperation._GetStatisticsCommand()
        self.store.get_request_executor().execute_command(command)

        statistics = command.result

        self.assertEqual(0, len(statistics.indexes))

    def test_can_reset(self):
        with self.store.open_session() as session:
            user1 = User()
            user1.name = "Marcin"
            session.store(user1, "users/1")
            session.save_changes()

        self.store.execute_index(UsersIndex())
        self.wait_for_indexing(self.store)

        command = GetStatisticsOperation._GetStatisticsCommand()
        self.store.get_request_executor().execute_command(command)

        statistics = command.result

        first_indexing_time = statistics.indexes[0].last_indexing_time

        index_name = UsersIndex().index_name

        # now reset index

        time.sleep(0.02)  # avoid the same millisecond

        self.store.maintenance.send(ResetIndexOperation(index_name))
        self.wait_for_indexing(self.store)

        command = GetStatisticsOperation._GetStatisticsCommand()
        self.store.get_request_executor().execute_command(command)

        statistics = command.result

        second_indexing_time = statistics.last_indexing_time
        self.assertLess(first_indexing_time, second_indexing_time)

    def test_can_stop_and_start(self):
        Users_ByName().execute(self.store)

        status = self.store.maintenance.send(GetIndexingStatusOperation())

        self.assertEqual(IndexRunningStatus.RUNNING, status.status)

        self.assertEqual(1, len(status.indexes))

        self.assertEqual(IndexRunningStatus.RUNNING, status.indexes[0].status)

        self.store.maintenance.send(StopIndexingOperation())

        status = self.store.maintenance.send(GetIndexingStatusOperation())

        self.assertEqual(IndexRunningStatus.PAUSED, status.status)

        self.assertEqual(IndexRunningStatus.PAUSED, status.indexes[0].status)

        self.store.maintenance.send(StartIndexingOperation())

        status = self.store.maintenance.send(GetIndexingStatusOperation())

        self.assertEqual(IndexRunningStatus.RUNNING, status.status)

        self.assertEqual(1, len(status.indexes))

        self.assertEqual(IndexRunningStatus.RUNNING, status.indexes[0].status)

        self.store.maintenance.send(StopIndexOperation(status.indexes[0].name))

        status = self.store.maintenance.send(GetIndexingStatusOperation())

        self.assertEqual(IndexRunningStatus.RUNNING, status.status)

        self.assertEqual(1, len(status.indexes))

        self.assertEqual(IndexRunningStatus.PAUSED, status.indexes[0].status)

    def test_get_index_names(self):
        with self.store.open_session() as session:
            session.store(User(name="Fitzchak"))
            session.store(User(name="Arek"))
            session.save_changes()

        with self.store.open_session() as session:
            stats: Optional[QueryStatistics] = None

            def _stats_callback(qs: QueryStatistics) -> None:
                nonlocal stats
                stats = qs

            users = list(
                session.query(object_type=User)
                .wait_for_non_stale_results()
                .statistics(_stats_callback)
                .where_equals("name", "Arek")
            )
            index_name = stats.index_name

        with self.store.open_session() as session:
            index_names = self.store.maintenance.send(GetIndexNamesOperation(0, 10))
            self.assertEqual(1, len(index_names))
            self.assertIn(index_name, index_names)

    def test_can_explain(self):
        with self.store.open_session() as session:
            session.store(User(name="Fitzchak"))
            session.store(User(name="Arek"))
            session.save_changes()

        with self.store.open_session() as session:

            def _stats_callback(qs: QueryStatistics) -> None:
                stats = qs

            users = list(session.query(object_type=User).statistics(_stats_callback).where_equals("name", "Arek"))
            users = list(session.query(object_type=User).statistics(_stats_callback).where_greater_than("age", 10))

            index_query = IndexQuery("from users")
            command = ExplainQueryCommand(self.store.conventions, index_query)

            self.store.get_request_executor().execute_command(command)

            explanations = command.result
            self.assertEqual(1, len(explanations))
            self.assertIsNotNone(explanations[0].index)
            self.assertIsNotNone(explanations[0].reason)

    def test_set_lock_mode_and_set_priority(self):
        Users_ByName().execute(self.store)

        with self.store.open_session() as session:
            session.store(User(name="Fitzchak"))
            session.store(User(name="Arek"))
            session.save_changes()

        with self.store.open_session() as session:
            users = list(
                session.query_index_type(Users_ByName, User).wait_for_non_stale_results().where_equals("name", "Arek")
            )
            self.assertEqual(1, len(users))

        indexes = self.store.maintenance.send(GetIndexesOperation(0, 128))
        self.assertEqual(1, len(indexes))

        index = indexes[0]
        stats = self.store.maintenance.send(GetIndexStatisticsOperation(index.name))

        self.assertEqual(stats.lock_mode, IndexLockMode.UNLOCK)
        self.assertEqual(stats.priority, IndexPriority.NORMAL)

        self.store.maintenance.send(SetIndexesLockOperation(IndexLockMode.LOCKED_IGNORE, index.name))
        self.store.maintenance.send(SetIndexesPriorityOperation(IndexPriority.LOW, index.name))

        stats = self.store.maintenance.send(GetIndexStatisticsOperation(index.name))

        self.assertEqual(IndexLockMode.LOCKED_IGNORE, stats.lock_mode)
        self.assertEqual(IndexPriority.LOW, stats.priority)

    def test_get_terms(self):
        with self.store.open_session() as session:
            session.store(User(name="Fitzchak"))
            session.store(User(name="Arek"))
            session.save_changes()

        with self.store.open_session() as session:
            stats: Optional[QueryStatistics] = None

            def _stats(s: QueryStatistics):
                nonlocal stats
                stats = s

            users = list(
                session.query(object_type=User)
                .wait_for_non_stale_results()
                .statistics(_stats)
                .where_equals("name", "Arek")
            )
            index_name = stats.index_name

        terms = self.store.maintenance.send(GetTermsOperation(index_name, "name", None, 128))

        self.assertEqual(2, len(terms))

        self.assertIn("arek", terms)
        self.assertIn("fitzchak", terms)
