import http
from unittest import TestCase

from ravendb.exceptions.cluster import NoLeaderException, NoLoaderException
from ravendb.exceptions.exception_dispatcher import ExceptionDispatcher
from ravendb.exceptions.raven_exceptions import RavenException


def _dispatch(type_as_string: str) -> RavenException:
    schema = ExceptionDispatcher.ExceptionSchema(
        url="http://localhost:8080",
        object_type=type_as_string,
        message="no leader",
        error="no leader",
    )
    schema.type = type_as_string
    return ExceptionDispatcher.get(schema, http.HTTPStatus.INTERNAL_SERVER_ERROR)


class TestExceptionDispatcher(TestCase):
    def test_no_leader_from_the_server_is_typed(self):
        # The map was keyed on "NoLoaderException", which the server never sends, so a real
        # no-leader failure arrived as a plain RavenException.
        exception = _dispatch("Raven.Client.Exceptions.Cluster.NoLeaderException")

        self.assertIsInstance(exception, NoLeaderException)

    def test_the_old_misspelled_name_is_still_importable(self):
        self.assertIs(NoLeaderException, NoLoaderException)

    def test_an_unknown_type_stays_a_raven_exception(self):
        exception = _dispatch("Raven.Client.Exceptions.Cluster.SomethingElseException")

        self.assertIs(RavenException, type(exception))
