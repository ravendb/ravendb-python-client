import os
import unittest
from typing import Dict, List, Optional

from ravendb.documents.operations.ai.ai_connection_string import AiConnectionString, AiModelType
from ravendb.documents.operations.ai.ai_task_identifier_helper import AiTaskIdentifierHelper
from ravendb.documents.operations.ai.open_ai_settings import OpenAiSettings
from ravendb.documents.operations.connection_string.get_connection_string_operation import (
    GetConnectionStringsOperation,
)
from ravendb.documents.operations.connection_string.put_connection_string_operation import PutConnectionStringOperation
from ravendb.exceptions.raven_exceptions import RavenException
from ravendb.serverwide.database_record import DatabaseRecord
from ravendb.serverwide.operations.common import CreateDatabaseOperation, DeleteDatabaseOperation
from ravendb.serverwide.operations.connection_strings import (
    GetServerWideConnectionStringsOperation,
    PutServerWideConnectionStringOperation,
    RemoveServerWideConnectionStringOperation,
    ServerWideConnectionString,
)
from ravendb.serverwide.server_operation_executor import ConnectionStringType
from ravendb.tests.test_base import TestBase


@unittest.skipIf(os.environ.get("RAVENDB_LICENSE") is None, "Insufficient license permissions. Skipping on CI/CD.")
class TestRavenDB24310(TestBase):
    # Server-wide connection strings outlive the test database and propagate to every database
    # created later, so each test removes the ones it puts and the databases it creates.

    def setUp(self):
        super().setUp()
        self._server_wide_connection_strings: Dict[str, AiConnectionString] = {}
        self._databases: List[str] = []

    def tearDown(self):
        for connection_string in self._server_wide_connection_strings.values():
            try:
                self.store.maintenance.server.send(RemoveServerWideConnectionStringOperation(connection_string))
            except Exception:
                pass

        for database in self._databases:
            try:
                self.store.maintenance.server.send(DeleteDatabaseOperation(database, True))
            except Exception:
                pass

        super().tearDown()

    @staticmethod
    def _new_ai_connection_string(name: str, identifier: Optional[str]) -> AiConnectionString:
        return AiConnectionString(
            name=name,
            identifier=identifier,
            openai_settings=OpenAiSettings(api_key="fake-key", model="test"),
            model_type=AiModelType.CHAT,
        )

    def _put_server_wide(self, server_wide: ServerWideConnectionString) -> None:
        self._server_wide_connection_strings[server_wide.name] = server_wide.connection_string
        self.store.maintenance.server.send(PutServerWideConnectionStringOperation(server_wide))

    def _get_server_wide(self, name: str) -> List[ServerWideConnectionString]:
        return self.store.maintenance.server.send(
            GetServerWideConnectionStringsOperation(name, ConnectionStringType.AI)
        ).results

    def _create_database(self, name: str) -> None:
        self._databases.append(name)
        self.store.maintenance.server.send(CreateDatabaseOperation(DatabaseRecord(name)))

    def _database_ai_connection_strings(self, database: str) -> Dict[str, AiConnectionString]:
        result = self.store.maintenance.for_database(database).send(GetConnectionStringsOperation())
        return result.ai_connection_strings or {}

    def test_server_wide_ai_connection_string_generates_identifier_when_missing(self):
        cs_name = "My Server Wide AI CS"
        self._put_server_wide(ServerWideConnectionString(self._new_ai_connection_string(cs_name, identifier=None)))

        expected_identifier = AiTaskIdentifierHelper.generate_identifier(cs_name)

        results = self._get_server_wide(cs_name)
        self.assertEqual(1, len(results))
        self.assertEqual(expected_identifier, results[0].connection_string.identifier)

        prefixed_name = ServerWideConnectionString.get_database_record_connection_string_name(cs_name)
        connection_strings = self._database_ai_connection_strings(self.store.database)
        self.assertEqual(expected_identifier, connection_strings[prefixed_name].identifier)

        new_db_name = self.store.database + "_new"
        self._create_database(new_db_name)
        connection_strings = self._database_ai_connection_strings(new_db_name)
        self.assertEqual(expected_identifier, connection_strings[prefixed_name].identifier)

    def test_ai_connection_string_rejects_missing_identifier_when_name_has_no_ascii_letters_or_digits(self):
        cs_name = "日本語"
        expected_error = (
            f"Can't generate an identifier from name '{cs_name}' because it contains no ASCII letters or digits"
        )

        self.assertRaisesWithMessageContaining(
            self._put_server_wide,
            RavenException,
            expected_error,
            ServerWideConnectionString(self._new_ai_connection_string(cs_name, identifier=None)),
        )

        self.assertEqual(0, len(self._get_server_wide(cs_name)))

        self.assertRaisesWithMessageContaining(
            self.store.maintenance.send,
            RavenException,
            expected_error,
            PutConnectionStringOperation(self._new_ai_connection_string(cs_name, identifier=None)),
        )

        self._put_server_wide(ServerWideConnectionString(self._new_ai_connection_string(cs_name, "nihongo")))
        self.store.maintenance.send(
            PutConnectionStringOperation(self._new_ai_connection_string("Database " + cs_name, identifier=None))
        )

    def test_server_wide_ai_connection_string_rejects_invalid_identifier(self):
        server_wide = ServerWideConnectionString(self._new_ai_connection_string("MyAiCS", "Invalid_Identifier"))

        with self.assertRaises(RavenException) as context:
            self._put_server_wide(server_wide)

        self.assertIn("Invalid connection string identifier", context.exception.args[0])
        self.assertIn("uppercase", context.exception.args[0])

        self.assertEqual(0, len(self._get_server_wide("MyAiCS")))

    def test_server_wide_ai_connection_string_rejects_identifier_used_by_database_connection_string(self):
        identifier = "shared-identifier"
        self.store.maintenance.send(
            PutConnectionStringOperation(self._new_ai_connection_string("DbLevelAiCS", identifier))
        )

        server_wide = ServerWideConnectionString(self._new_ai_connection_string("MyAiCS", identifier))

        self.assertRaisesWithMessageContaining(
            self._put_server_wide,
            RavenException,
            f"The identifier '{identifier}' is already used by connection string 'DbLevelAiCS' "
            f"in database '{self.store.database}'",
            server_wide,
        )

        prefixed_name = ServerWideConnectionString.get_database_record_connection_string_name("MyAiCS")
        self.assertNotIn(prefixed_name, self._database_ai_connection_strings(self.store.database))

        self.assertEqual(0, len(self._get_server_wide("MyAiCS")))

        server_wide.excluded_databases = [self.store.database]
        self._put_server_wide(server_wide)

        server_wide.excluded_databases = None
        self.assertRaisesWithMessageContaining(
            self._put_server_wide,
            RavenException,
            f"The identifier '{identifier}' is already used",
            server_wide,
        )

    def test_server_wide_ai_connection_string_can_be_updated_with_same_identifier(self):
        identifier = "my-identifier"
        ai_cs = self._new_ai_connection_string("MyAiCS", identifier)
        self._put_server_wide(ServerWideConnectionString(ai_cs))

        ai_cs.openai_settings.model = "test-2"
        self._put_server_wide(ServerWideConnectionString(ai_cs))

        prefixed_name = ServerWideConnectionString.get_database_record_connection_string_name("MyAiCS")
        connection_strings = self._database_ai_connection_strings(self.store.database)
        self.assertEqual(identifier, connection_strings[prefixed_name].identifier)
        self.assertEqual("test-2", connection_strings[prefixed_name].openai_settings.model)

    def test_server_wide_ai_connection_string_update_without_identifier_keeps_stored_identifier(self):
        cs_name = "My Server Wide AI CS"
        prefixed_name = ServerWideConnectionString.get_database_record_connection_string_name(cs_name)
        generated_identifier = AiTaskIdentifierHelper.generate_identifier(cs_name)

        self._put_server_wide(ServerWideConnectionString(self._new_ai_connection_string(cs_name, identifier=None)))

        connection_strings = self._database_ai_connection_strings(self.store.database)
        self.assertEqual(generated_identifier, connection_strings[prefixed_name].identifier)

        updated = self._new_ai_connection_string(cs_name, identifier=None)
        updated.openai_settings.api_key = "rotated-key"
        self._put_server_wide(ServerWideConnectionString(updated))

        connection_strings = self._database_ai_connection_strings(self.store.database)
        self.assertEqual(generated_identifier, connection_strings[prefixed_name].identifier)
        self.assertEqual("rotated-key", connection_strings[prefixed_name].openai_settings.api_key)

        results = self._get_server_wide(cs_name)
        self.assertEqual(1, len(results))
        self.assertEqual(generated_identifier, results[0].connection_string.identifier)

        self._put_server_wide(
            ServerWideConnectionString(self._new_ai_connection_string(cs_name, identifier="explicit-identifier"))
        )

        connection_strings = self._database_ai_connection_strings(self.store.database)
        self.assertEqual("explicit-identifier", connection_strings[prefixed_name].identifier)

    def test_server_wide_ai_connection_string_rejects_identifier_used_by_another_server_wide_connection_string(self):
        # The C# test runs without a database. Here the test database is excluded instead, so the
        # server-wide check is the one that rejects the identifier, not the check of the database.
        identifier = "shared-identifier"
        excluded = [self.store.database]
        self._put_server_wide(
            ServerWideConnectionString(self._new_ai_connection_string("FirstAiCS", identifier), excluded)
        )

        self.assertRaisesWithMessageContaining(
            self._put_server_wide,
            RavenException,
            f"The identifier '{identifier}' is already used by server-wide connection string 'FirstAiCS'",
            ServerWideConnectionString(self._new_ai_connection_string("SecondAiCS", identifier), excluded),
        )

        self.assertEqual(0, len(self._get_server_wide("SecondAiCS")))

        self._put_server_wide(
            ServerWideConnectionString(self._new_ai_connection_string("SecondAiCS", "other-identifier"), excluded)
        )

        new_db_name = self.store.database + "_new"
        self._create_database(new_db_name)
        self.assertEqual(2, len(self._database_ai_connection_strings(new_db_name)))
