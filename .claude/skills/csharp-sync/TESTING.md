# Testing Conventions

## Infrastructure

- All tests extend `TestBase` from `ravendb/tests/test_base.py`.
- `self.store` is pre-configured with a fresh database per test — no manual setup needed.
- Run tests: `.venv1\Scripts\python.exe -m unittest <module.path> -v`

## Assertion helpers

| Helper | When to use |
|---|---|
| `self.assertRaisesWithMessageContaining(func, ExType, "substring")` | Server-side exceptions — the dispatcher includes the full C# stack trace, so exact match is fragile |
| `self.assertRaisesWithMessage(func, ExType, "exact")` | Client-side exceptions only (e.g. `ValueError`) |
| `self.assertFalse(x)` | Prefer over `assertIsNone` when the server may return `[]` instead of `null` |
| `self.wait_for_indexing(self.store)` | Call after creating indexes, before querying |

## Test file placement

| C# test location | Python target |
|---|---|
| `SlowTests.Server.Documents.*` | `ravendb/tests/operations_tests/` |
| `SlowTests.Client.*` | `ravendb/tests/jvm_migrated_tests/client_tests/` |
| `FastTests.Client.Attachments.*` | `ravendb/tests/jvm_migrated_tests/attachments_tests/` |
| `SlowTests.Issues.RavenDB_XXXXX` | `ravendb/tests/jvm_migrated_tests/issues_tests/` |

## Test structure

```python
class TestMyFeature(TestBase):
    def setUp(self):
        super().setUp()

    def test_should_do_something(self):
        # 1. Arrange — configure the feature
        config = MyConfig(name="test")
        self.store.maintenance.send(ConfigureMyFeatureOperation(config))

        # 2. Act
        with self.store.open_session() as session:
            session.store({"field": "value"}, "docs/1")
            session.save_changes()

        # 3. Assert
        with self.store.open_session() as session:
            doc = session.load("docs/1")
            self.assertEqual("value", doc["field"])
```

## Index query projection pattern

When an index projects custom fields, create a lightweight result class:

```python
class _IndexResult:
    def __init__(self, Id=None, Errors=None):
        self.Id = Id
        self.Errors = Errors
```

Then query with `select_fields(_IndexResult, "Id", "Errors")`.

## Common gotchas

- **Datetime**: always `datetime.datetime.now(datetime.timezone.utc)`, never `utcnow()`.
- **Empty vs None**: server may return `[]` instead of `null` for empty collections — use `assertFalse`/`assertTrue` not `assertIsNone`/`assertIsNotNone`.
- **Exception messages**: the dispatcher wraps the full server error (message + stack trace) into the exception string — always use `assertRaisesWithMessageContaining` with a meaningful substring.
- **`RavenException.__init__`**: stores message as plain string in `args[0]`, not `(message, cause)` tuple.
- **Async operations**: use `store.maintenance.send_async(op)` → `op.wait_for_completion()` → `op.fetch_operations_status()["Result"]`.

