"""
DocumentQuery: and_also(wrap_previous_query_clauses=True) wraps preceding WHERE
tokens in a subclause so AND has the correct precedence relative to OR.

C# reference: FastTests/Client/Queries/QueryTests.cs
  Query_CreateClausesForQueryDynamicallyWithOnBeforeQueryEvent
"""

from ravendb.tests.test_base import TestBase


class Article:
    def __init__(self, title: str = "", description: str = "", is_deleted: bool = False):
        self.title = title
        self.description = description
        self.is_deleted = is_deleted


class TestRavenDBAndAlsoWrapClauses(TestBase):
    def setUp(self):
        super().setUp()
        with self.store.open_session() as session:
            session.store(Article(title="foo", description="bar", is_deleted=False), "articles/1")
            session.store(Article(title="foo", description="bar", is_deleted=True), "articles/2")
            session.save_changes()

    def test_and_also_accepts_wrap_previous_query_clauses_parameter(self):
        """
        C# spec: query.AndAlso(wrapPreviousQueryClauses: true) is a named parameter
        that wraps preceding clauses in parentheses before appending AND.
        and_also(wrap_previous_query_clauses=True) must be accepted without error.
        """
        with self.store.open_session() as session:
            q = session.advanced.document_query(object_type=Article)
            q.and_also(wrap_previous_query_clauses=True)

    def test_and_also_with_wrap_produces_subclause_rql(self):
        """
        C# spec: QueryTests.Query_CreateClausesForQueryDynamicallyWithOnBeforeQueryEvent
          builds: search(Title, $p0) or search(Description, $p1)
          then adds: andAlso(wrapPreviousQueryClauses: true).WhereEquals(IsDeleted, true)
          expected RQL: "from 'Articles' where (search(Title, $p0) or search(Description, $p1)) and IsDeleted = $p2"
        """
        with self.store.open_session() as session:
            q = session.advanced.document_query(object_type=Article)
            q = q.search("title", "foo")
            q = q.or_else()
            q = q.search("description", "bar")
            q = q.and_also(wrap_previous_query_clauses=True)
            q = q.where_equals("is_deleted", True)

            rql = q.index_query.query
            self.assertIn(
                "(search(",
                rql,
                f"RQL should open subclause before search(), got: {rql!r}",
            )
            self.assertIn(
                ") and ",
                rql,
                f"RQL should close subclause before AND, got: {rql!r}",
            )
            self.assertIn(
                "is_deleted",
                rql,
                f"RQL should contain is_deleted after AND, got: {rql!r}",
            )

    def test_and_also_with_wrap_returns_one_filtered_result(self):
        """
        C# spec: expected results: 1 document (is_deleted=true only).
        (search(title, foo) OR search(description, bar)) AND is_deleted=true
        matches only articles/2.
        """
        with self.store.open_session() as session:
            q = session.advanced.document_query(object_type=Article)
            q = q.search("title", "foo")
            q = q.or_else()
            q = q.search("description", "bar")
            q = q.and_also(wrap_previous_query_clauses=True)
            q = q.where_equals("is_deleted", True)
            results = list(q)

        self.assertEqual(
            1,
            len(results),
            f"(title=foo OR description=bar) AND is_deleted=true should return 1 result, got {len(results)}",
        )

    def test_and_also_without_or_works(self):
        """
        and_also() works correctly when there is no preceding OR to wrap.
        """
        with self.store.open_session() as session:
            q = session.advanced.document_query(object_type=Article)
            q = q.where_equals("title", "foo")
            q = q.and_also()
            q = q.where_equals("is_deleted", True)
            results = list(q)

        self.assertEqual(
            1,
            len(results),
            "Simple AND (no preceding OR) should return exactly 1 result",
        )
