# Naming & Code Conventions

## Naming map

| C# | Python |
|---|---|
| `PascalCase` class | `PascalCase` class |
| `PascalCase` method | `snake_case` method |
| `PascalCase` property | `snake_case` attribute |
| JSON keys in `ToJson()` | `"PascalCase"` in `to_json()` |
| JSON keys in `FromJson()` | `"PascalCase"` in `from_json()` |
| `IOperation<T>` | `IOperation[T]` |
| `IMaintenanceOperation` | `IMaintenanceOperation` |
| `IVoidMaintenanceOperation` | `VoidMaintenanceOperation` |
| `null` | `None` |
| `TimeSpan.FromDays(n)` | `timedelta(days=n)` |
| `DateTime.UtcNow` | `datetime.datetime.now(datetime.timezone.utc)` — NEVER `utcnow()` |

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

## Import style

- Absolute imports from `ravendb.*`.
- Group: stdlib → third-party → project, separated by blank lines.
- Use `TYPE_CHECKING` guard for circular-import-prone types.

## Verifying against C# source

When unsure about a field name, method signature, or serialization key, fetch the original C# file:

```
https://raw.githubusercontent.com/ravendb/ravendb/refs/heads/v7.2/src/Raven.Client/<path>.cs
```



