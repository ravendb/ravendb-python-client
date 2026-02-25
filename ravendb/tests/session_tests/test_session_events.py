"""
Session events: BeforeDeleteEventArgs and BeforeQueryEventArgs expose the current
session without infinite recursion.

C# reference: FastTests/Client/Events.cs
  Before_Delete_Session_Listener_With_Delete_Inside
"""

from ravendb.documents.session.event_args import (
    BeforeDeleteEventArgs,
    BeforeQueryEventArgs,
    BeforeStoreEventArgs,
)
from ravendb.tests.test_base import TestBase


class User:
    def __init__(self, name: str = ""):
        self.name = name


class TestEventArgsSessionProperty(TestBase):
    def setUp(self):
        super().setUp()

    # ------------------------------------------------------------------ #
    #  BeforeDeleteEventArgs.session returns the current session          #
    # ------------------------------------------------------------------ #

    def test_before_delete_event_args_session_returns_session(self):
        """
        C# spec: BeforeDeleteEventArgs.Session returns the current session so
        handlers can perform additional session operations.
        """
        with self.store.open_session() as session:
            session.store(User(name="Alice"), "users/1")
            session.save_changes()

        session_from_args = []

        def handler(args: BeforeDeleteEventArgs):
            session_from_args.append(args.session)

        with self.store.open_session() as session:
            session.add_before_delete(handler)
            user = session.load("users/1", User)
            session.delete(user)
            session.save_changes()

        self.assertEqual(1, len(session_from_args), "handler should have been called once")
        self.assertIsNotNone(session_from_args[0], "args.session should return the session object")

    def test_before_delete_handler_can_cascade_delete_via_args_session(self):
        """
        C# spec (Events.cs — Before_Delete_Session_Listener_With_Delete_Inside):
          handler receives BeforeDeleteEventArgs, loads users/2 via args.session,
          then calls args.session.delete(user2) to cascade the deletion.
          After save_changes(), both users/1 and users/2 are deleted.
        """
        with self.store.open_session() as session:
            session.store(User(name="Foo"), "users/1")
            session.store(User(name="Bar"), "users/2")
            session.save_changes()

        with self.store.open_session() as session:

            def handler(args: BeforeDeleteEventArgs):
                user2 = args.session.load("users/2", User)
                args.session.delete(user2)

            session.add_before_delete(handler)
            user1 = session.load("users/1", User)
            session.delete(user1)
            session.save_changes()

        with self.store.open_session() as session:
            user1 = session.load("users/1", User)
            self.assertIsNone(user1, "users/1 should have been deleted")
            user2 = session.load("users/2", User)
            self.assertIsNone(user2, "users/2 should have been cascade-deleted via args.session.delete()")

    # ------------------------------------------------------------------ #
    #  BeforeQueryEventArgs.session returns the current session           #
    # ------------------------------------------------------------------ #

    def test_before_query_event_args_session_returns_session(self):
        """
        C# spec: BeforeQueryEventArgs.Session returns the current session.
        """
        with self.store.open_session() as session:
            session.store(User(name="Carol"), "users/3")
            session.save_changes()

        session_from_args = []

        def handler(args: BeforeQueryEventArgs):
            session_from_args.append(args.session)

        with self.store.open_session() as session:
            session.add_before_query(handler)
            _ = list(session.query(object_type=User))

        self.assertEqual(1, len(session_from_args), "before_query handler should have been called once")
        self.assertIsNotNone(session_from_args[0], "args.session should return the session object")

    # ------------------------------------------------------------------ #
    #  BeforeStoreEventArgs.session baseline                              #
    # ------------------------------------------------------------------ #

    def test_before_store_event_args_session_property_works(self):
        """
        Baseline: BeforeStoreEventArgs.session correctly returns the session.
        Confirms the recursion bug is specific to BeforeDeleteEventArgs and
        BeforeQueryEventArgs, not all event args.
        """
        with self.store.open_session() as session:
            session.store(User(name="Dave"), "users/4")
            session.save_changes()

        session_from_args = []

        def handler(args: BeforeStoreEventArgs):
            session_from_args.append(args.session)

        with self.store.open_session() as session:
            session.add_before_store(handler)
            session.store(User(name="Eve"), "users/5")
            session.save_changes()

        self.assertEqual(1, len(session_from_args), "before_store handler should be called")
        self.assertIsNotNone(session_from_args[0], "BeforeStoreEventArgs.session should return the session object")
