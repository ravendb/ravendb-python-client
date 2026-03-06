import json
import os
import unittest

from ravendb.documents.indexes.definitions import FieldStorage, IndexDefinition, IndexFieldOptions
from ravendb.documents.operations.indexes import PutIndexesOperation, ResetIndexOperation
from ravendb.documents.operations.schema_validation import (
    ConfigureSchemaValidationOperation,
    GetSchemaValidationConfiguration,
    SchemaDefinition,
    SchemaValidationConfiguration,
    StartSchemaValidationOperation,
    ValidateSchemaResult,
)
from ravendb.exceptions.raven_exceptions import RavenException, SchemaValidationException
from ravendb.tests.test_base import TestBase

# Minimal JSON Schema used across tests
_SCHEMA_REQUIRE_NAME = json.dumps(
    {
        "type": "object",
        "properties": {"name": {"type": "string"}},
        "required": ["name"],
    }
)

_SCHEMA_REQUIRE_AGE = json.dumps(
    {
        "type": "object",
        "properties": {"age": {"type": "integer"}},
        "required": ["age"],
    }
)

# Schema used in indexing tests: Prop must be ≤ 10 chars
_SCHEMA_PROP_MAX_LENGTH_10 = json.dumps(
    {
        "properties": {"Prop": {"maxLength": 10}},
    }
)

# Schema used in indexing tests: Prop ≤ 10 chars AND must match "^something", plus Prop1 required
_SCHEMA_PROP_MULTIPLE_RULES = json.dumps(
    {
        "properties": {"Prop": {"maxLength": 10, "pattern": "^something"}},
        "required": ["Prop1"],
    }
)

# Map that projects Schema.GetErrorsFor(doc) into an Errors field (LINQ syntax)
_MAP_VALIDATE_DOCUMENT = (
    "from doc in docs "
    'where MetadataFor(doc)["@collection"] != "@hilo" '
    "select new { Id = doc.Id, Errors = Schema.GetErrorsFor(doc) }"
)


class User:
    def __init__(self, Id: str = None, name: str = None, age: int = None):
        self.Id = Id
        self.name = name
        self.age = age


class TestSchemaValidation(TestBase):
    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()
        TestBase.delete_all_topology_files()

    # ------------------------------------------------------------------
    # Ported from SchemaValidationBasicTests.cs
    # ------------------------------------------------------------------

    @unittest.skipIf(os.environ.get("RAVENDB_LICENSE") is None, "Insufficient license permissions. Skipping on CI/CD.")
    def test_can_configure_schema_validation(self):
        """Configure a schema for Users collection and read it back."""
        schema_def = SchemaDefinition(schema=_SCHEMA_REQUIRE_NAME)
        config = SchemaValidationConfiguration(validators_per_collection={"Users": schema_def})

        self.store.maintenance.send(ConfigureSchemaValidationOperation(config))

        result = self.store.maintenance.send(GetSchemaValidationConfiguration())
        self.assertIsNotNone(result)
        self.assertIn("Users", result.validators_per_collection)
        stored = result.validators_per_collection["Users"]
        self.assertEqual(_SCHEMA_REQUIRE_NAME, stored.schema)

    @unittest.skipIf(os.environ.get("RAVENDB_LICENSE") is None, "Insufficient license permissions. Skipping on CI/CD.")
    def test_schema_validation_blocks_invalid_document(self):
        """Saving a document that violates the schema raises SchemaValidationException."""
        schema_def = SchemaDefinition(schema=_SCHEMA_REQUIRE_NAME)
        config = SchemaValidationConfiguration(validators_per_collection={"Users": schema_def})
        self.store.maintenance.send(ConfigureSchemaValidationOperation(config))

        with self.assertRaises(SchemaValidationException):
            with self.store.open_session() as session:
                # User without 'name' — violates the schema
                user = User(age=30)
                session.store(user, "users/1")
                session.save_changes()

    @unittest.skipIf(os.environ.get("RAVENDB_LICENSE") is None, "Insufficient license permissions. Skipping on CI/CD.")
    def test_schema_validation_allows_valid_document(self):
        """Saving a document that satisfies the schema succeeds."""
        schema_def = SchemaDefinition(schema=_SCHEMA_REQUIRE_NAME)
        config = SchemaValidationConfiguration(validators_per_collection={"Users": schema_def})
        self.store.maintenance.send(ConfigureSchemaValidationOperation(config))

        with self.store.open_session() as session:
            user = User(name="Alice")
            session.store(user, "users/1")
            session.save_changes()

        with self.store.open_session() as session:
            loaded = session.load("users/1", User)
            self.assertEqual("Alice", loaded.name)

    @unittest.skipIf(os.environ.get("RAVENDB_LICENSE") is None, "Insufficient license permissions. Skipping on CI/CD.")
    def test_disabled_schema_allows_invalid_document(self):
        """When the schema is disabled, invalid documents are accepted."""
        schema_def = SchemaDefinition(schema=_SCHEMA_REQUIRE_NAME, disabled=True)
        config = SchemaValidationConfiguration(validators_per_collection={"Users": schema_def})
        self.store.maintenance.send(ConfigureSchemaValidationOperation(config))

        with self.store.open_session() as session:
            # No 'name' field — would fail if schema were active
            user = User(age=99)
            session.store(user, "users/1")
            session.save_changes()

        with self.store.open_session() as session:
            loaded = session.load("users/1", User)
            self.assertEqual(99, loaded.age)

    # ------------------------------------------------------------------
    # Ported from SchemaValidationOperationTests.cs
    # ------------------------------------------------------------------

    def test_start_schema_validation_operation_returns_result(self):
        """StartSchemaValidationOperation completes and returns a ValidateSchemaResult."""
        with self.store.open_session() as session:
            session.store(User(name="Alice"), "users/1")
            session.store(User(name="Bob"), "users/2")
            session.save_changes()

        params = StartSchemaValidationOperation.Parameters(
            schema_definition=_SCHEMA_REQUIRE_NAME,
            collection="Users",
        )
        op = self.store.maintenance.send_async(StartSchemaValidationOperation(params))
        op.wait_for_completion()

        status = op.fetch_operations_status()
        result = ValidateSchemaResult.from_json(status["Result"])

        self.assertIsNotNone(result)
        self.assertEqual(2, result.validated_count)
        self.assertEqual(0, result.error_count)
        self.assertEqual({}, result.errors)

    def test_start_schema_validation_operation_reports_errors(self):
        """StartSchemaValidationOperation reports documents that violate the schema."""
        # Insert documents without a schema active so they bypass write-time validation
        with self.store.open_session() as session:
            session.store(User(name="Alice"), "users/1")
            session.store(User(age=30), "users/2")  # missing 'name' — will fail audit
            session.save_changes()

        params = StartSchemaValidationOperation.Parameters(
            schema_definition=_SCHEMA_REQUIRE_NAME,
            collection="Users",
        )
        op = self.store.maintenance.send_async(StartSchemaValidationOperation(params))
        op.wait_for_completion()

        status = op.fetch_operations_status()
        result = ValidateSchemaResult.from_json(status["Result"])

        self.assertEqual(2, result.validated_count)
        self.assertEqual(1, result.error_count)
        self.assertIn("users/2", result.errors)

    def test_start_schema_validation_operation_with_max_error_messages(self):
        """max_error_messages caps the number of error entries returned."""
        with self.store.open_session() as session:
            for i in range(5):
                session.store(User(age=i), f"users/{i + 1}")  # all missing 'name'
            session.save_changes()

        params = StartSchemaValidationOperation.Parameters(
            schema_definition=_SCHEMA_REQUIRE_NAME,
            collection="Users",
            max_error_messages=2,
        )
        op = self.store.maintenance.send_async(StartSchemaValidationOperation(params))
        op.wait_for_completion()

        status = op.fetch_operations_status()
        result = ValidateSchemaResult.from_json(status["Result"])

        self.assertEqual(5, result.validated_count)
        self.assertEqual(5, result.error_count)
        # Only 2 error messages should be stored despite 5 failures
        self.assertLessEqual(len(result.errors), 2)

    # ------------------------------------------------------------------
    # Progress tests
    # ------------------------------------------------------------------
    def test_validate_schema_start_etag_skips_earlier_docs(self):
        """Using start_etag from a first run skips already-validated documents.

        Ported from ValidateSchemaOperation_WhenSettingEtagOnNonSharded_ShouldStartFromTheEtag.
        """
        # Schema that requires 'name' to be a string — both docs violate it (age-only)
        with self.store.open_session() as session:
            session.store(User(age=1), "users/1")  # inserted first → lower etag
            session.store(User(age=2), "users/2")  # inserted second → higher etag
            session.save_changes()

        params1 = StartSchemaValidationOperation.Parameters(
            schema_definition=_SCHEMA_REQUIRE_NAME,
            collection="Users",
            max_documents_to_validate=1,  # only scan the first doc
        )
        op1 = self.store.maintenance.send_async(StartSchemaValidationOperation(params1))
        op1.wait_for_completion()
        result1 = ValidateSchemaResult.from_json(op1.fetch_operations_status()["Result"])

        self.assertEqual(1, result1.validated_count)
        self.assertEqual(1, result1.error_count)
        self.assertGreater(result1.last_etag, 0)

        # Second run starts from where the first left off
        params2 = StartSchemaValidationOperation.Parameters(
            schema_definition=_SCHEMA_REQUIRE_NAME,
            collection="Users",
            start_etag=result1.last_etag + 1,
        )
        op2 = self.store.maintenance.send_async(StartSchemaValidationOperation(params2))
        op2.wait_for_completion()
        result2 = ValidateSchemaResult.from_json(op2.fetch_operations_status()["Result"])

        # The second run should only see users/2
        self.assertEqual(1, result2.validated_count)
        self.assertEqual(1, result2.error_count)
        self.assertIn("users/2", result2.errors)
        self.assertNotIn("users/1", result2.errors)

    def test_validate_schema_max_documents_to_validate_caps_scan(self):
        """max_documents_to_validate limits how many documents are scanned."""
        with self.store.open_session() as session:
            for i in range(6):
                session.store(User(age=i), f"users/{i + 1}")  # all missing 'name'
            session.save_changes()

        params = StartSchemaValidationOperation.Parameters(
            schema_definition=_SCHEMA_REQUIRE_NAME,
            collection="Users",
            max_documents_to_validate=3,
        )
        op = self.store.maintenance.send_async(StartSchemaValidationOperation(params))
        op.wait_for_completion()
        result = ValidateSchemaResult.from_json(op.fetch_operations_status()["Result"])

        # Only 3 of the 6 documents should have been scanned
        self.assertEqual(3, result.validated_count)
        self.assertEqual(3, result.error_count)
        # last_etag is set so a follow-up run can continue from here
        self.assertGreater(result.last_etag, 0)


# ---------------------------------------------------------------------------
# Helper result class for indexing tests
# ---------------------------------------------------------------------------
class _IndexResult:
    """Projection class for the Errors field produced by the schema-validation index."""

    def __init__(self, Id: str = None, Errors: list = None):
        self.Id = Id
        self.Errors = Errors


class TestSchemaValidationIndexing(TestBase):
    """
    Ported from SlowTests.Server.Documents.Indexing.SchemaValidationIndexingTests (C#).

    These tests verify that an index can project Schema.GetErrorsFor(doc) into an
    Errors field, and that the server correctly populates it based on either an
    index-level or database-level schema definition.
    """

    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()
        TestBase.delete_all_topology_files()

    def _put_schema_index(self, index_name: str, schema_definitions: dict = None) -> IndexDefinition:
        """Create and register an index that projects Schema.GetErrorsFor(doc) → Errors."""
        index_def = IndexDefinition(
            name=index_name,
            maps={_MAP_VALIDATE_DOCUMENT},
            fields={"Errors": IndexFieldOptions(storage=FieldStorage.YES)},
            schema_definitions=schema_definitions,
        )
        self.store.maintenance.send(PutIndexesOperation(index_def))
        return index_def

    # ------------------------------------------------------------------
    # IndexingSchemaErrors_WhenFailsOneRule_ShouldGetTheError
    # ------------------------------------------------------------------
    def test_indexing_schema_errors_when_fails_one_rule_should_get_the_error(self):
        """
        An index with a schema_definition that limits Prop to 10 chars should
        project a non-null Errors list for the violating document and null for the valid one.
        """
        invalid_doc_id = "testobjs/invalid"
        valid_doc_id = "testobjs/valid"

        index_def = self._put_schema_index(
            "IndexWithSchemaValidation",
            schema_definitions={"TestObjs": _SCHEMA_PROP_MAX_LENGTH_10},
        )

        with self.store.open_session() as session:
            session.store({"Prop": "0123456789a"}, invalid_doc_id)  # 11 chars — violates maxLength:10
            session.store({"Prop": "01"}, valid_doc_id)
            session.save_changes()

        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            results = list(
                session.query_index(index_def.name, _IndexResult).select_fields(_IndexResult, "Id", "Errors")
            )
            by_id = {r.Id: r for r in results}

            # Server returns None or [] for a valid document
            self.assertFalse(by_id[valid_doc_id].Errors)

            errors = by_id[invalid_doc_id].Errors
            self.assertTrue(errors)
            self.assertEqual(1, len(errors))
            self.assertIn("Prop", errors[0])

    # ------------------------------------------------------------------
    # IndexingSchemaErrors_WhenFailsMultipleRules_ShouldGetTheErrors
    # ------------------------------------------------------------------
    def test_indexing_schema_errors_when_fails_multiple_rules_should_get_the_errors(self):
        """
        When a document violates multiple schema rules (maxLength, pattern, required),
        all error messages should appear in the projected Errors list.
        """
        index_def = self._put_schema_index(
            "IndexWithSchemaValidation",
            schema_definitions={"TestObjs": _SCHEMA_PROP_MULTIPLE_RULES},
        )

        with self.store.open_session() as session:
            session.store({"Prop": "0123456789a"}, "testobjs/1")
            session.store({"Prop": "0123456789a"}, "testobjs/2")
            session.save_changes()

        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            results = list(
                session.query_index(index_def.name, _IndexResult).select_fields(_IndexResult, "Id", "Errors")
            )
            for result in results:
                self.assertIsNotNone(result.Errors)
                # Expect 3 violations: maxLength, pattern, required Prop1
                self.assertEqual(3, len(result.Errors))

    # ------------------------------------------------------------------
    # IndexingSchemaErrors_WhenDefineSchemaOnMetadata_ShouldReject
    # ------------------------------------------------------------------
    def test_indexing_schema_errors_when_define_schema_on_metadata_should_reject(self):
        """
        Defining a schema rule on the @metadata key should be rejected by the server
        with a RavenException containing 'Define a schema validation on metadata is not allowed'.
        """
        schema_on_metadata = json.dumps({"properties": {"@metadata": {"maxLength": 10}}})

        index_def = IndexDefinition(
            name="IndexWithSchemaValidation",
            maps={_MAP_VALIDATE_DOCUMENT},
            fields={"Errors": IndexFieldOptions(storage=FieldStorage.YES)},
            schema_definitions={"TestObjs": schema_on_metadata},
        )

        self.assertRaisesWithMessageContaining(
            self.store.maintenance.send,
            RavenException,
            "Define a schema validation on metadata is not allowed",
            PutIndexesOperation(index_def),
        )

    # ------------------------------------------------------------------
    # IndexingSchemaErrors_WhenSchemaDefinedInDatabase_ShouldIndexErrors
    # ------------------------------------------------------------------
    @unittest.skipIf(os.environ.get("RAVENDB_LICENSE") is None, "Insufficient license permissions. Skipping on CI/CD.")
    def test_indexing_schema_errors_when_schema_defined_in_database_should_index_errors(self):
        """
        When no schema_definitions are set on the index itself, but a database-level
        schema is configured via ConfigureSchemaValidationOperation, resetting the index
        should cause it to pick up the DB schema and project errors correctly.
        """
        invalid_doc_id = "testobjs/invalid"
        valid_doc_id = "testobjs/valid"

        # Index without schema_definitions — no validation yet
        index_def = self._put_schema_index("IndexWithSchemaValidation")

        with self.store.open_session() as session:
            session.store({"Prop": "0123456789a"}, invalid_doc_id)
            session.store({"Prop": "01"}, valid_doc_id)
            session.save_changes()

        self.wait_for_indexing(self.store)

        # Now configure a DB-level schema for the TestObjs collection
        config = SchemaValidationConfiguration(
            validators_per_collection={"TestObjs": SchemaDefinition(schema=_SCHEMA_PROP_MAX_LENGTH_10)}
        )
        self.store.maintenance.send(ConfigureSchemaValidationOperation(config))

        # Reset the index so it re-indexes all documents with the new schema
        self.store.maintenance.send(ResetIndexOperation(index_def.name))
        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            results = list(
                session.query_index(index_def.name, _IndexResult).select_fields(_IndexResult, "Id", "Errors")
            )
            by_id = {r.Id: r for r in results}

            # Server returns None or [] for a valid document
            self.assertFalse(by_id[valid_doc_id].Errors)

            errors = by_id[invalid_doc_id].Errors
            self.assertTrue(errors)
            self.assertEqual(1, len(errors))
            self.assertIn("Prop", errors[0])
