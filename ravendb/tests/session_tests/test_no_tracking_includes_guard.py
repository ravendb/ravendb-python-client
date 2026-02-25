"""
No-tracking session: using includes raises when session.no_tracking is True.

C# reference: SlowTests/Issues/RavenDB_21339.cs
  Using_Includes_In_Non_Tracking_Session_Should_Throw
"""

from ravendb.documents.session.misc import SessionOptions
from ravendb.tests.test_base import TestBase


class Manager:
    def __init__(self, name: str = None):
        self.name = name


class Employee:
    def __init__(self, name: str = None, manager_id: str = None):
        self.name = name
        self.manager_id = manager_id


class TestRavenDB21339(TestBase):
    def setUp(self):
        super().setUp()

    def _populate(self):
        with self.store.open_session() as session:
            session.store(Manager(name="Boss"), "managers/1")
            session.store(Employee(name="Alice", manager_id="managers/1"), "employees/1")
            session.save_changes()

    def test_load_with_includes_in_no_tracking_session_should_throw(self):
        """
        session.load() with an include builder in a no_tracking session must
        raise an exception, not silently ignore the includes."""
        self._populate()

        with self.store.open_session(session_options=SessionOptions(no_tracking=True)) as session:
            with self.assertRaises(Exception) as ctx:
                session.load(
                    "employees/1",
                    Employee,
                    includes=lambda b: b.include_documents("manager_id"),
                )
            self.assertIn("Cannot register includes when no_tracking is enabled", str(ctx.exception))

    def test_query_with_include_in_no_tracking_session_should_throw(self):
        """
        query.include() in a no_tracking session must raise an exception,
        not silently add the include to an unused tracking cache."""
        self._populate()

        with self.store.open_session(session_options=SessionOptions(no_tracking=True)) as session:
            with self.assertRaises(Exception) as ctx:
                list(session.query(object_type=Employee).include("manager_id").wait_for_non_stale_results())
            self.assertIn("Cannot register includes when no_tracking is enabled", str(ctx.exception))

    def test_document_query_with_include_in_no_tracking_session_should_throw(self):
        """
        C# spec: session.Advanced.DocumentQuery<Product>().Include(x => x.Supplier).ToList()
        in a no_tracking session must raise InvalidOperationException.
        """
        self._populate()

        with self.store.open_session(session_options=SessionOptions(no_tracking=True)) as session:
            with self.assertRaises(Exception) as ctx:
                list(session.advanced.document_query(object_type=Employee).include("manager_id"))
            self.assertIn("Cannot register includes when no_tracking is enabled", str(ctx.exception))
