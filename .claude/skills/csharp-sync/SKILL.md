---
name: csharp-sync
description: Migrates RavenDB C# client features to the Python client. Use when porting C# diffs, patches, or test files to Python, batching sync work, or writing migration tests.
allowed-tools: "Read Edit Write Grep Glob Bash WebFetch Agent"
argument-hint: "<patch-file-or-feature-description>"
effort: max
model: opus
disable-model-invocation: true
---

# C# → Python Client Sync

Migrate features from the RavenDB C# client into this Python client codebase.
Input is a raw `.patch` / `.diff` from the C# repo. Output is working Python code with tests.

## Current state
- Branch: !`git branch --show-current`
- Recent syncs: !`git log --oneline -5 --grep="RDBC"`

## Input

If arguments were provided (`$ARGUMENTS`), use them as the starting point:
- If it's a file path, read it as the diff/patch input
- If it's a feature name, fetch the relevant C# source to begin triage

## Pipeline

1. **Triage** — read the diff, list every changed file, group into batches
2. **Plan** — present the batch plan to the user and wait for approval before writing any code
3. **Implement** — port each batch following [CONVENTIONS.md](CONVENTIONS.md)
4. **Test** — migrate or write tests following [TESTING.md](TESTING.md)

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
5. **Search for existing Python equivalents** — for each changed C# file, Grep for the class name, operation name, or endpoint path in the Python codebase:
   - If a Python version exists, compare it with the C# version to identify only the delta to port.
   - If the Python version has intentional divergences (different architecture, Pythonic patterns), preserve them.
6. Group remaining changes into **batches** by feature area. Good boundaries:
   - New operation class (e.g. `ConfigureRemoteAttachmentsOperation`)
   - New model / DTO cluster (e.g. settings + configuration classes)
   - New exception type + dispatcher wiring
   - Test migration for an already-implemented feature
7. Create a **checklist** — one `- [ ]` per batch, ordered by dependency (models → operations → tests).
   Use TodoWrite to create tasks for each batch. Mark each as completed before moving to the next.

### Batch sizing

- Target **≤ 300 lines of Python** per batch.
- If a single C# file maps to > 300 lines, split by class or logical section.
- Tests are their own batch, listed after the implementation batch they cover.
- If a batch modifies existing models/DTOs that have downstream consumers, include the consumer updates in the same batch — even if it exceeds the 300-line target. A broken intermediate state is worse than a large batch.

## Step 2 — Present the plan for approval

Before writing any code, present the full migration plan to the user. The plan should include:

1. **Summary** — what the diff covers and what will be ported vs. skipped.
2. **Batch list** — numbered batches with:
   - Batch name / feature area
   - Which C# files map to which Python files (new or existing)
   - Estimated scope (new file, modify existing, add fields, etc.)
   - Any risks or ambiguities flagged during triage
3. **Skipped items** — C# changes filtered out and why (C#-specific, dependency not ported, etc.)
4. **Open questions** — anything that needs the user's decision before implementation.

**Wait for the user to approve the plan before proceeding to Step 3.** The user may reorder batches, split/merge them, or ask to skip certain items. Adjust the TodoWrite tasks accordingly.

## Step 3 — Implement each batch

Only proceed after user approval from Step 2.

Follow the naming rules and patterns in [CONVENTIONS.md](CONVENTIONS.md).

### Porting principle

Port the *behavior*, not the *syntax*. If the Python client already handles something in a Pythonic way (context managers, generators, keyword arguments), preserve that pattern even if the C# implementation looks different. The goal is feature parity, not code parity.

When porting a C# class, **always fetch the full source from GitHub** — diffs alone often lack constructor signatures, base classes, or `ToJson`/`FromJson` methods needed for a correct port.

For every edit:
1. Use Grep and Glob to find ALL downstream callers, implementations, and tests.
2. Update every affected file — missing a downstream change is a critical failure.
3. After editing, verify imports resolve and no existing tests are broken.
4. If unsure about a field or method, check the **RavenDB docs** for the canonical behavior.

### When unsure

1. If the C# pattern has no direct Python equivalent → ask the user.
2. If the feature partially exists → check `git log` for prior sync commits to understand how similar cases were handled.
3. If a dependency isn't ported yet → skip and note it as a prerequisite in the batch checklist.

### Session changes require extra caution

The session subsystem (`ravendb/documents/session/`) spans 25+ modules and ~13K lines. Before porting session-related C# changes:
1. Read the target Python module thoroughly — session modules are tightly coupled.
2. Trace the full call chain from session API → command generation → request execution.
3. Session batches should be smaller (≤150 lines) due to higher coupling risk.
4. Always run the full session test suite after changes, not just new tests.

### Streaming operations

Streaming operations (bulk insert, subscriptions, query streaming) use generators and `concurrent.futures` — NOT the standard operation pattern. Fetch the full Python source for the existing streaming infrastructure before porting.

### Verification gate

After completing all batches, run:
1. `black --check .` — fix any formatting issues.
2. `python -m unittest <new_test_modules> -v` — all new tests must pass.
3. `python -m unittest discover` on affected test directories — confirm no regressions.

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

## Step 4 — Write tests

Follow the infrastructure and patterns in [TESTING.md](TESTING.md).

## If a batch fails

- If tests fail due to a bug in the port → fix in the same batch.
- If the feature can't be ported (missing dependency, server version mismatch) → revert the batch, add a `# todo: requires <dependency>` comment, and continue with remaining batches.
- Never leave broken code committed — each batch must be independently valid.
