"""
Tests for the typed exceptions the 7.2.5 and 7.2.6 patches added: the LimitType enum,
LicenseLimitException coming back from a 402 instead of a bare RavenException, and
QueryToolFailedException for a failed agent query tool.
"""

import unittest

from ravendb.exceptions.commercial import LicenseLimitException, LimitType
from ravendb.exceptions.exception_dispatcher import ExceptionDispatcher
from ravendb.exceptions.raven_exceptions import AiException, QueryToolFailedException, RavenException


class TestLimitType(unittest.TestCase):
    def test_the_limits_7_2_5_added(self):
        self.assertEqual("ServerWideConnectionStrings", LimitType.SERVER_WIDE_CONNECTION_STRINGS.value)
        self.assertEqual("CdcSink", LimitType.CDC_SINK.value)
        self.assertEqual("Sso", LimitType.SSO.value)

    def test_a_limit_parses_from_the_name_the_server_uses(self):
        self.assertEqual(LimitType.QUEUE_SINK, LimitType("QueueSink"))
        self.assertEqual(LimitType.SCHEMA_VALIDATION, LimitType("SchemaValidation"))

    def test_every_member_stringifies_to_its_wire_name(self):
        for member in LimitType:
            self.assertEqual(member.value, str(member))


class TestLicenseLimitException(unittest.TestCase):
    def test_it_is_a_raven_exception(self):
        self.assertIsInstance(LicenseLimitException("nope"), RavenException)

    def test_it_can_carry_the_limit_it_hit(self):
        exception = LicenseLimitException("nope", LimitType.QUEUE_SINK)

        self.assertEqual(LimitType.QUEUE_SINK, exception.limit_type)

    def test_the_dispatcher_returns_it_for_a_402(self):
        schema = ExceptionDispatcher.ExceptionSchema(
            url="http://localhost:8080",
            object_type="Raven.Client.Exceptions.Commercial.LicenseLimitException",
            message="no",
            error="Your license doesn't support using the queue sink feature.",
        )

        exception = ExceptionDispatcher.get(schema, 402)

        self.assertIsInstance(exception, LicenseLimitException)
        self.assertIn("queue sink", str(exception))
        # The server does not put the limit on the wire, so it stays unset here, exactly
        # as it does in the C# client.
        self.assertIsNone(exception.limit_type)

    def test_a_402_from_another_exception_type_is_left_alone(self):
        schema = ExceptionDispatcher.ExceptionSchema(
            url="http://localhost:8080",
            object_type="Raven.Client.Exceptions.RavenException",
            message="no",
            error="something else",
        )

        self.assertNotIsInstance(ExceptionDispatcher.get(schema, 402), LicenseLimitException)


class TestQueryToolFailedException(unittest.TestCase):
    def test_it_is_an_ai_exception(self):
        self.assertIsInstance(QueryToolFailedException("nope"), AiException)

    def test_the_dispatcher_returns_it(self):
        schema = ExceptionDispatcher.ExceptionSchema(
            url="http://localhost:8080",
            object_type="Raven.Client.Exceptions.QueryToolFailedException",
            message="no",
            error="The agent's query tool could not run the query.",
        )

        exception = ExceptionDispatcher.get(schema, 500)

        self.assertIsInstance(exception, QueryToolFailedException)
        self.assertIn("query tool", str(exception))
