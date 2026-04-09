---
name: csharp-sync
description: Migrates RavenDB C# client features to the Python client. Use when porting C# diffs, patches, or test files to Python, batching sync work, or writing migration tests.
---

# C# → Python Client Sync

Migrate features from the RavenDB C# client into this Python client codebase.
Input is a raw `.patch` / `.diff` from the C# repo. Output is working Python code with tests.

## Pipeline

1. **Triage** — read the diff, list every changed file, group into batches
2. **Implement** — port each batch following [CONVENTIONS.md](CONVENTIONS.md)
3. **Test** — migrate or write tests following [TESTING.md](TESTING.md)

## Reference sources

When triaging or implementing, cross-reference with the upstream C# source and RavenDB docs.

### C# source (GitHub raw)

Base URL for the `v7.2` branch:
```
https://raw.githubusercontent.com/ravendb/ravendb/refs/heads/v7.2/src/Raven.Client/
```

To read a C# file, build the URL from its namespace path:
- `Documents/Operations/Attachments/PutAttachmentOperation.cs`
- `Documents/Commands/Batches/PutAttachmentCommandData.cs`
- `Documents/Indexes/IndexDefinition.cs`
- `Exceptions/Documents/DocumentDoesNotExistException.cs`

To discover what's inside a directory, fetch the GitHub API:
```
https://api.github.com/repos/ravendb/ravendb/contents/src/Raven.Client/Documents/Operations?ref=v7.2
```
This returns a JSON array of `{name, path, type}` entries — use it to list files before fetching specific ones.

For **tests**, the base path is:
```
https://raw.githubusercontent.com/ravendb/ravendb/refs/heads/v7.2/test/SlowTests/
```
With the API equivalent:
```
https://api.github.com/repos/ravendb/ravendb/contents/test/SlowTests?ref=v7.2
```

### RavenDB documentation

Base URL: `https://docs.ravendb.net/7.2`

Key sections to check during triage:
- `document-extensions/attachments/` — attachment operations
- `documents/schema-validation/` — JSON schema validation
- `indexes/` — index definitions, schema in indexes
- `ai-integration/` — AI / embeddings features

Use docs to determine whether a C# change is:
1. **A documented feature** → must be ported
2. **C#-specific plumbing** (e.g. `IDisposable`, `Span<T>`) → skip
3. **Related to a feature not yet synced** → defer to a later batch

## Step 1 — Triage the diff

1. Read the full diff. For each changed C# file write a one-line summary.
2. For unfamiliar changes, **fetch the full C# source** from GitHub to understand context.
3. **Check RavenDB docs** to confirm the feature is documented and understand its scope.
4. **Filter out** changes that are C#-specific or relate to features not yet in the Python client.
5. Group remaining changes into **batches** by feature area. Good boundaries:
   - New operation class (e.g. `ConfigureRemoteAttachmentsOperation`)
   - New model / DTO cluster (e.g. settings + configuration classes)
   - New exception type + dispatcher wiring
   - Test migration for an already-implemented feature
6. Create a **checklist** — one `- [ ]` per batch, ordered by dependency (models → operations → tests).

### Batch sizing

- Target **≤ 300 lines of Python** per batch.
- If a single C# file maps to > 300 lines, split by class or logical section.
- Tests are their own batch, listed after the implementation batch they cover.

## Step 2 — Implement each batch

Follow the naming rules and patterns in [CONVENTIONS.md](CONVENTIONS.md).

When porting a C# class, **always fetch the full source from GitHub** — diffs alone often lack constructor signatures, base classes, or `ToJson`/`FromJson` methods needed for a correct port.

For every edit:
1. Use `codebase-retrieval` to find ALL downstream callers, implementations, and tests.
2. Update every affected file — missing a downstream change is a critical failure.
3. After editing, verify imports resolve and no existing tests are broken.
4. If unsure about a field or method, check the **RavenDB docs** for the canonical behavior.

### Key files to touch per feature

| What | Where |
|---|---|
| New operation / model | `ravendb/documents/operations/<feature>/__init__.py` |
| New exception | `ravendb/exceptions/raven_exceptions.py` or subpackage |
| Exception dispatcher map | `ravendb/exceptions/exception_dispatcher.py` |
| DatabaseRecord fields | `ravendb/serverwide/database_record.py` |
| IndexDefinition fields | `ravendb/documents/indexes/definitions.py` |
| Statistics fields | `ravendb/documents/operations/statistics.py` |
| Session-level changes | `ravendb/documents/session/` submodules |
| Bulk insert changes | `ravendb/documents/bulk_insert_operation.py` |
| Module exports | `ravendb/__init__.py` |

## Step 3 — Write tests

Follow the infrastructure and patterns in [TESTING.md](TESTING.md).


