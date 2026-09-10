# Unported C# client surface

What the Python client still does not carry from the C# `v7.2` client, after the
RDBC-1099 sync (C# `7.2.3-custom-72` -> `7.2.5-custom-72`), its follow-up passes, and the
`7.2.6-custom-72` sync on top.

Keep this short. It is a pointer list, not an archive: the patches and the C# source are
the record, and pasting diffs here only rots. Update it when something moves.

Last reviewed: 2026-09-10, against `ravendb/v7.2` @ `fbc98d1` plus the RDBC-1099 branch.

Everything in both patches that has a Python home is ported.

## No Python analogue, and none is planned

These exist only because of how C# works. There is nothing to port.

| C# | Why |
|---|---|
| `Json/Serialization/JsonDeserializationClient.cs` | Generated deserializer registry; Python parses per class in `from_json` |
| `Http/RequestExecutor.cs` cancellation-token plumbing | The Python client is synchronous |
| `Util/LeaveOpenStream.cs`, `DatabaseSmuggler.StreamContentWithConfirmation` | Works around `StreamContent` disposing the stream it wraps; `requests` does not |
| `Documents/Operations/Backups/IS3Settings.cs` | Interface; the fields live on both settings classes already |
| `#if NETCOREAPP` `[JsonIgnore]` attributes on AI configurations | Attribute-only change |
| `Compile(preferInterpretation: true)` in `LinqPathProvider`, `RangeFacet`, `TimeSeriesQueryVisitor`, `JavascriptConversionExtensions` (7.2.6) | Tunes how .NET compiles expression trees. Python builds queries from strings and lambdas it never compiles |
| `Http/HttpCache.cs` `unsafe` on `SetNotFound` (7.2.6) | A C# keyword |
| `NewtonsoftJson/DefaultRavenContractResolver.cs` (7.2.6) | Works around a Newtonsoft 13.0.4 value-provider regression |

## Already correct here, so the C# fix does not apply

- **`ConversationResult` reading a string answer (7.2.6).** C# needed a branch on
  `typeof(TAnswer)` to read `Response` as a string rather than an object. Python's
  `from_json` takes the parsed value as-is, so both shapes already worked.
- **`MoreLikeThisToken.AddAlias` (7.2.6).** C# had to override it because
  `MoreLikeThisToken : WhereToken` there. Here it derives from `QueryToken`, so the alias
  rewriting never touched it.
- **`AiUsage.ReasoningTokens` (7.2.5).** The field and its round trip already existed; the
  C# change only fixed C#'s hand-written writer.

## Server-side logic that happens to live in the client assembly

Public in C# because the server links the same assembly. No client calls them, and this
client keeps task configuration to plain `to_json` / `from_json`.

- `PullReplicationAsSink.IsEqualTo` / `AllowedPathsEqual`.
- `QueueSinkConfiguration.Validate` / `Compare`, `CdcSinkConfiguration.Validate` /
  `Compare` / `CollectAllTablesFlat`, and the `IDatabaseTask` members on both.
- `QueueSinkConfigurationCompareDifferences`, `CdcSinkConfigurationCompareDifferences`.
- `AzureServiceBusSinkSource.ValidateScript` / `ValidateEntry` / `TryParseSubscription`.
  The two public encoders (`Queue`, `Subscription`) are ported.

## Blocked on a subsystem the Python client does not have

| C# | Blocked on |
|---|---|
| `Documents/Changes/ChangeNotification.cs` (`TrafficWatchChangeBase`) | `internal` in C#, and delivered over a server-wide changes connection this client does not open. `ravendb/changes/` is per-database only |
| `Documents/Replication/Messages/*` | The server-to-server replication wire protocol. A client never speaks it |
| `InMemoryDocumentSessionOperations.UpdateEntityDocumentInfo` composite change vectors | No `ClientChangeVectorUtils`; the session takes the cluster-transaction index from the batch result instead |
| `Smuggler/OfflineMigration*`, `Smuggler/ShardedSmuggler*` | Offline migration and sharding are not in this client |
| **Session JsonPatch (7.2.6)**: `JsonPatchCommandData`, `SessionPatchBehavior`, `DocumentConventions.SessionPatchBehavior`, `InMemoryDocumentSessionOperations.Patch` | This client has no JsonPatch support at all, and its session patch API takes string paths rather than `Expression<Func<T, U>>`. The C# change is ~240 lines of expression-tree-to-JSON-pointer translation with no Python counterpart. Porting it means first adding a JsonPatch command and a path-expression story |

## Known gaps outside the patches

- **`from_json` strictness in `operations/backups/settings.py`.** Twelve `from_json`
  methods across 19 settings classes index with `json_dict["Key"]`, so a payload missing
  any key raises where C#'s reflection-based deserializer would keep the field default.
  `S3Settings` and `AzureSettings` were fixed because the new conversions run through
  them; `LocalSettings`, `GlacierSettings`, `FtpSettings`, `GoogleCloudSettings` and the
  rest still have it. Same family as the `DatabaseRecord` and `AutoIndexDefinition` fixes.
- **`EmbeddingsGenerationConfiguration.from_json` requires `ChunkingOptionsForQuerying`**
  and hands `None` straight to `ChunkingOptions.from_json`, so its own `to_json` output
  does not round trip when that section is unset. Same family again.
- **`DatabaseRecord` is a partial mirror.** It carries `cdc_sinks` and `queue_sinks` now,
  but still has no `queue_etls`, `snowflake_etls` or `elastic_search_etls`.
- **`DatabaseSmugglerOptions.IsShard`** is `internal` with `[ForceJsonSerialization]`, so
  C# puts `"IsShard": false` on the wire. Omitted here; the server default matches.
- **`DatabaseItemType.Counters`** was removed in C# 7.x and kept in the Python enum for
  callers written against older servers. It is not in the default selection.
- **CDC schema-discovery and mapping-preview operations are `internal` in C#.** They were
  ported on request, so Python exposes slightly more CDC surface than the C# public API.
