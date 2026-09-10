# Unported C# client surface

What the Python client still does not carry from the C# `v7.2` client, after the
RDBC-1099 sync (C# `7.2.3-custom-72` -> `7.2.5-custom-72`) and its follow-up passes.

Keep this short. It is a pointer list, not an archive: the patch and the C# source are
the record, and pasting diffs here only rots. Update it when something moves.

Last reviewed: 2026-09-10, against `ravendb/v7.2` @ `fbc98d1` plus the RDBC-1099 branch.

Everything in the 7.2.5 patch that has a Python home is now ported, including the pieces
first deferred as blocked: Queue Sink, the database smuggler and its typed results, the
CDC Sink schema-discovery and mapping-preview operations, `LimitType` with a typed
`LicenseLimitException`, and the S3/Azure settings conversions.

## No Python analogue, and none is planned

These exist only because of how C# works. There is nothing to port.

| C# | Why |
|---|---|
| `Json/Serialization/JsonDeserializationClient.cs` | Generated deserializer registry; Python parses per class in `from_json` |
| `Http/RequestExecutor.cs` cancellation-token plumbing | The Python client is synchronous |
| `Util/LeaveOpenStream.cs`, `DatabaseSmuggler.StreamContentWithConfirmation` | Works around `StreamContent` disposing the stream it wraps; `requests` does not |
| `Documents/Operations/Backups/IS3Settings.cs` | Interface; the fields live on both settings classes already |
| `#if NETCOREAPP` `[JsonIgnore]` attributes on AI configurations | Attribute-only change |

## Server-side logic that happens to live in the client assembly

Public in C# because the server links the same assembly. No client calls them, and this
client keeps task configuration to plain `to_json` / `from_json`.

- `PullReplicationAsSink.IsEqualTo` / `AllowedPathsEqual` - lets the server notice a sink
  task changed and restart it.
- `QueueSinkConfiguration.Validate` / `Compare`, `CdcSinkConfiguration.Validate` /
  `Compare` / `CollectAllTablesFlat`, and the `IDatabaseTask` members on both.
- `QueueSinkConfigurationCompareDifferences`, `CdcSinkConfigurationCompareDifferences` -
  `internal`, feed those comparisons.
- `AzureServiceBusSinkSource.ValidateScript` / `ValidateEntry` / `TryParseSubscription` -
  `internal`. The two public encoders (`Queue`, `Subscription`) are ported.

## Blocked on a subsystem the Python client does not have

| C# | Blocked on |
|---|---|
| `Documents/Changes/ChangeNotification.cs` (`TrafficWatchChangeBase`) | `internal` in C#, and delivered over a server-wide changes connection this client does not open. `ravendb/changes/` is per-database only |
| `Documents/Replication/Messages/*` | The server-to-server replication wire protocol. A client never speaks it |
| `InMemoryDocumentSessionOperations.UpdateEntityDocumentInfo` composite change vectors | No `ClientChangeVectorUtils`; the session takes the cluster-transaction index from the batch result in `update_session_after_save_changes` instead, so the C# bug has no counterpart here |
| `Smuggler/OfflineMigration*`, `Smuggler/ShardedSmuggler*` | Offline migration and sharding are not in this client |

## Known gaps outside the patch

- **`from_json` strictness in `operations/backups/settings.py`.** Twelve `from_json`
  methods across 19 settings classes index with `json_dict["Key"]`, so a payload missing
  any key raises where C#'s reflection-based deserializer would keep the field default.
  `S3Settings` and `AzureSettings` were fixed because the new conversions run through
  them; `LocalSettings`, `GlacierSettings`, `FtpSettings`, `GoogleCloudSettings` and the
  rest still have it. Same family as the `DatabaseRecord` and `AutoIndexDefinition` fixes.
- **`DatabaseRecord` is a partial mirror.** It carries `cdc_sinks` and `queue_sinks` now,
  but still has no `queue_etls`, `snowflake_etls` or `elastic_search_etls`.
- **`DatabaseSmugglerOptions.IsShard`** is `internal` with `[ForceJsonSerialization]`, so
  C# puts `"IsShard": false` on the wire. Omitted here; the server default matches.
- **`DatabaseItemType.Counters`** was removed in C# 7.x and kept in the Python enum for
  callers written against older servers. It is not in the default selection.
