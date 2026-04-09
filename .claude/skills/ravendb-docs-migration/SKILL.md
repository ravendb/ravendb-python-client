# Skill: Migrating RavenDB Documentation from C# to Python

## Goal

Translate RavenDB documentation articles (MDX format) from C# to Python, producing
tested code samples and a ready-to-publish `_<article>-python.mdx` file.

---

## Workflow

### 1. Fetch the C# original

Download the raw MDX from the `ravendb/docs` repo on GitHub, e.g.:
```
https://raw.githubusercontent.com/ravendb/docs/refs/heads/main/docs/ai-integration/generating-embeddings/content/_overview-csharp.mdx
```

### 2. Create a tracking file (`docs-samples/<article>-python.md`)

For each C# code block in the article:
- Paste the C# original
- Write the Python translation directly below it
- Add checkboxes: `[ ] Sample translated`, `[ ] Test written`, `[ ] Test passing`

At the bottom, add a **"Language Discrepancies"** section listing every
prose/content fix the Python version needs (see § Translation Rules below).

### 3. Write tests (`ravendb/tests/embeddings_generation_tests/test_docs_<article>_samples.py`)

- **Do NOT use `TestBase`** — it requires tricky environment variables.
- Connect directly to `localhost:8080`, database `PythonDocsTest`.
- Each sample gets its own `unittest.TestCase` class with `setUp` / `tearDown`
  that creates and cleans up connection strings and ongoing tasks.
- Helper functions (`_make_store()`, `_make_config()`, etc.) go at module level.
- Run with: `.venv\Scripts\python -m pytest <path> -v`

### 4. Assemble the final MDX (`docs-samples/generating-embeddings-content/_<article>-python.mdx`)

- Start from the C# MDX structure — keep every Panel, ContentFrame, Admonition,
  image, link, and prose paragraph **identical** unless a Python-specific fix is needed.
- Replace only the code blocks (```csharp → ```python) with the tested translations.
- Replace the Syntax panel class/enum definitions with Python equivalents.
- Apply the content fixes from the discrepancies list.

---

## Translation Rules (C# → Python)

| C# pattern | Python equivalent |
|---|---|
| `PascalCase` properties (`task.TaskState`) | `snake_case` (`task.task_state`) |
| `PascalCase` variables (`var getOngoingTaskOp`) | `snake_case` (`get_ongoing_task_op`) |
| Enum values (`OngoingTaskType.EmbeddingsGeneration`) | `UPPER_SNAKE_CASE` (`OngoingTaskType.EMBEDDINGS_GENERATION`) |
| `TimeSpan.FromDays(90)` | `timedelta(days=90)` (from `datetime`) |
| `new ClassName { Prop = val }` (object initialiser) | `ClassName(prop=val)` (constructor kwargs) |
| `PutConnectionStringOperation<AiConnectionString>(...)` (generics) | `PutConnectionStringOperation(...)` (no generics) |
| `EtlConfiguration<AiConnectionString>` (base class) | `AbstractAiIntegrationConfiguration` |
| Explicit cast `(Type)store.Maintenance.Send(...)` | No cast — `store.maintenance.send(...)` returns the typed object |
| `@"..."` verbatim strings | `"""..."""` triple-quoted strings |
| `// comment` | `# comment` |
| `tasks.Count` | `len(tasks)` |
| `using` directives (implicit) | Explicit `from ravendb... import ...` block at top of each sample |
| "overloads" in prose | Replace with "alternative signatures" or "optional parameters" |

### Python client naming gotchas

- Connection string field is `openai_settings` (not `open_ai_settings`).
- `AiConnectionString` requires `model_type=AiModelType.TEXT_EMBEDDINGS`.
- Server-side scripts inside `EmbeddingsTransformation.script` remain **JavaScript**
  (executed by RavenDB server) — only the surrounding config code is Python.

### Prose content fixes for non-Python-idiomatic text

The C# articles sometimes contain prose, terminology, or explanations that don't
apply to Python. These must be handled with **minimal, targeted changes** — never
rewrite whole paragraphs unless absolutely necessary.

**What to fix:**
- **"overloads"** → replace with "alternative signatures" or "optional parameters"
  (Python doesn't have method overloading).
- **"cast" / "explicitly cast the result"** → remove or replace with
  "the result is already typed" (Python client returns correctly typed objects).
- **Generics in prose** (e.g. "pass the generic type parameter") → remove the
  sentence or reword to describe the Python constructor call.
- **C#-specific class names in prose** (e.g. `EtlConfiguration<AiConnectionString>`)
  → replace with the Python equivalent (`AbstractAiIntegrationConfiguration`).
- **`TimeSpan` mentions in prose** → replace with `timedelta`.
- **`null`** → replace with `None`.
- **Property casing in prose** (e.g. "set the `EmbeddingsPathConfigurations` property")
  → update to snake_case (`embeddings_path_configurations`).

**What NOT to fix:**
- Studio panel descriptions — these are UI-focused and language-agnostic; keep identical.
- Server-side JavaScript references — the scripts run on the RavenDB server regardless
  of client language; keep all JS method names, parameters, and explanations as-is.
- Links, images, Admonitions, and structural MDX components — keep identical.
- Prose that is already language-neutral (most of the article) — don't touch it.

**Principle:** If a sentence reads correctly for a Python developer without changes,
leave it alone. Only fix what would actively confuse or mislead a Python reader.

---

## File Map

```
docs-samples/
├── generating-embeddings-overview-python.md          # Tracking: overview samples
├── generating-embeddings-task-python.md               # Tracking: task samples
└── generating-embeddings-content/
    ├── _overview-python.mdx                           # Final MDX: overview article
    └── _embeddings-generation-task-python.mdx         # Final MDX: task article

ravendb/tests/embeddings_generation_tests/
├── test_docs_overview_samples.py                      # Tests for overview samples
└── test_docs_task_samples.py                          # Tests for task samples
```

---

## Test Infrastructure Pattern

```python
SERVER_URL = "http://localhost:8080"
DATABASE_NAME = "PythonDocsTest"

def _make_store() -> DocumentStore:
    store = DocumentStore(urls=[SERVER_URL], database=DATABASE_NAME)
    store.initialize()
    return store

class TestDocsSampleX(unittest.TestCase):
    def setUp(self):
        self.store = _make_store()
        self._connection_string = ...
        self.store.maintenance.send(PutConnectionStringOperation(self._connection_string))
        self._task_id = None

    def tearDown(self):
        if self._task_id is not None:
            try:
                self.store.maintenance.send(
                    DeleteOngoingTaskOperation(self._task_id, OngoingTaskType.EMBEDDINGS_GENERATION)
                )
            except Exception:
                pass
        try:
            self.store.maintenance.send(RemoveConnectionStringOperation(self._connection_string))
        except Exception:
            pass
        self.store.close()

    def test_sample(self):
        result = self.store.maintenance.send(AddEmbeddingsGenerationOperation(config))
        self._task_id = result.task_id
        self.assertGreater(self._task_id, 0)
```

---

## Checklist for each new article migration

- [ ] C# original fetched and reviewed
- [ ] Tracking `.md` created with all samples translated
- [ ] Discrepancies list filled out
- [ ] Tests written and passing
- [ ] Final `.mdx` assembled with Python samples and prose fixes
- [ ] Differences summary reviewed

