# Naming & Code Conventions

See CLAUDE.md for base code style, JSON conventions, and common gotchas. This file covers only migration-specific conventions beyond the baseline.

## Naming map

| C# | Python |
|---|---|
| `PascalCase` class | `PascalCase` class |
| `PascalCase` method | `snake_case` method |
| `PascalCase` property | `snake_case` attribute |
| `IOperation<T>` | `IOperation[T]` |
| `IMaintenanceOperation` | `IMaintenanceOperation` |
| `IVoidMaintenanceOperation` | `VoidMaintenanceOperation` |
| `null` | `None` |
| `TimeSpan.FromDays(n)` | `timedelta(days=n)` |

## C# → Python pattern map

These C# patterns have no 1:1 equivalent — use the Python strategy shown:

| C# Pattern | Python Strategy |
|-------------|----------------|
| Method overloading | Optional params with `None` defaults + `if` guards |
| `async/await` | **Synchronous** — this codebase is sync-only; use `concurrent.futures.ThreadPoolExecutor` where needed |
| `IDisposable` | `__enter__`/`__exit__` context managers |
| `event Action<T>` | `List[Callable[[EventArgs], None]]` with `add_`/`remove_` registration methods |
| `lock(obj)` | `with self._lock:` using `threading.Lock`, `RLock`, or `Semaphore` |
| Fluent builder API | Methods returning `self` for chaining |
| `Span<T>`, `ReadOnlySpan` | Skip — no Python equivalent |
| `Generic<T>` with constraints | `TypeVar("T", bound=Base)` |
| Lazy initialization | Double-check locking with `threading.RLock` |

## Operation class pattern

```python
class MyNewOperation(IMaintenanceOperation[MyResult]):
    def __init__(self, config: MyConfig):
        self._config = config

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[MyResult]:
        return self._MyCommand(self._config)

    class _MyCommand(RavenCommand[MyResult], RaftCommand):
        def __init__(self, config: MyConfig):
            super().__init__(MyResult)
            self._config = config

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            request = requests.Request(
                "PUT", f"{node.url}/databases/{node.database}/my-endpoint"
            )
            request.data = self._config.to_json()
            return request

        def set_response(self, response: str, from_cache: bool) -> None:
            if response:
                self.result = MyResult.from_json(json.loads(response))

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()
```

## Model / DTO pattern

```python
class MyConfig:
    def __init__(self, name: str = None, items: Optional[Dict] = None):
        self.name = name
        self.items = items

    def to_json(self) -> dict:
        result = {}
        if self.name is not None:
            result["Name"] = self.name
        if self.items is not None:
            result["Items"] = {k: v.to_json() for k, v in self.items.items()}
        return result

    @classmethod
    def from_json(cls, json_dict: dict) -> "MyConfig":
        obj = cls.__new__(cls)
        obj.name = json_dict.get("Name")
        # parse nested objects ...
        return obj
```

## Exception wiring

1. Create the class inheriting `RavenException` in `ravendb/exceptions/`.
2. Register it in `exception_dispatcher.py` → `_EXCEPTION_MAP` using the **short C# type name** as key (e.g. `"SchemaValidationException"`).
3. `RavenException.__init__` stores message as a plain string in `args[0]` — never a tuple.

## Enum pattern

```python
class MyEnum(enum.Enum):
    VALUE_ONE = "ValueOne"   # match the C# string / JSON value
    VALUE_TWO = "ValueTwo"
```

## Verifying against C# source

When unsure about a field name, method signature, or serialization key, fetch the original C# file:

```
https://raw.githubusercontent.com/ravendb/ravendb/refs/heads/v7.2/src/Raven.Client/<path>.cs
```



