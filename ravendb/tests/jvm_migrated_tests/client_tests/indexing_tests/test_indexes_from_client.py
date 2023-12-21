import time

from ravendb.documents.operations.statistics import GetStatisticsOperation
from ravendb.documents.indexes.index_creation import IndexCreation
from ravendb.documents.queries.more_like_this import MoreLikeThisOptions
from ravendb.documents.operations.indexes import GetIndexNamesOperation, DeleteIndexOperation, ResetIndexOperation
from ravendb.documents.indexes.definitions import FieldIndexing, FieldStorage
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
