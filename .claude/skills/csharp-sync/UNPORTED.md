# Unported C# client surface

What the Python client still does not carry from the C# `v7.2` client, after the
RDBC-1099 sync (C# `7.2.3-custom-72` -> `7.2.5-custom-72`).

Keep this short. It is a pointer list, not an archive: the patch and the C# source are
the record, and pasting diffs here only rots. Update it when something moves.

Last reviewed: 2026-09-08, against `ravendb/v7.2` @ `fbc98d1` plus the RDBC-1099 branch.

## No Python analogue, and none is planned

These exist only because of how C# works. There is nothing to port.

| C# | Why |
|---|---|
| `Json/Serialization/JsonDeserializationClient.cs` | Generated deserializer registry; Python parses per class in `from_json` |
| `Http/RequestExecutor.cs` cancellation-token plumbing | The Python client is synchronous |
| `Util/LeaveOpenStream.cs`, `DatabaseSmuggler.StreamContentWithConfirmation` | Works around `StreamContent` disposing the stream it wraps |
| `Documents/Operations/Backups/IS3Settings.cs` | Interface; the fields live on both settings classes already |
| `#if NETCOREAPP` `[JsonIgnore]` attributes on AI configurations | Attribute-only change |
| `Extensions/RemoteAttachmentExtensions.cs` | S3-settings conversion helpers, never ported |

## Server-side logic that happens to live in the client assembly

Public in C# because the server links the same assembly. No client calls them.

- `PullReplicationAsSink.IsEqualTo` / `AllowedPathsEqual` - lets the server notice a sink
  task changed and restart it.
- `QueueSinkConfiguration.Validate` / `Compare`, `CdcSinkConfiguration.Validate` /
  `Compare` / `CollectAllTablesFlat`, and the `IDatabaseTask` members on both. The server
  validates; this client keeps task configuration to plain `to_json` / `from_json`.
- `QueueSinkConfigurationCompareDifferences`, `CdcSinkConfigurationCompareDifferences` -
  `internal`, feed those comparisons.
- `AzureServiceBusSinkSource.ValidateScript` / `ValidateEntry` / `TryParseSubscription` -
  `internal`. The two public encoders (`Queue`, `Subscription`) are ported.

## `internal` in C#, so outside the public API on both sides

- `Documents/Operations/CdcSink/Schema/*` and `GetCdcSinkSchemaOperation` - Studio's
  schema-discovery view.
- `Documents/Operations/CdcSink/Test/*` - Studio's "Test" button.

Porting either would give Python more surface than C# exposes, and neither can be
exercised without a SQL Server or PostgreSQL CDC source.

## Blocked on a subsystem the Python client does not have

| C# | Blocked on |
|---|---|
| `Documents/Changes/ChangeNotification.cs` (`TrafficWatchChangeBase`) | No Traffic Watch changes API |
| `Documents/Replication/Messages/*` | No replication TCP wire protocol |
| `Exceptions/Commercial/LimitType.cs` | No `LimitType` enum anywhere in the client |
| `InMemoryDocumentSessionOperations.UpdateEntityDocumentInfo` composite change vectors | No `ClientChangeVectorUtils`; the session takes the cluster-transaction index from the batch result instead, so the C# bug has no counterpart here |

## Ported, but only partly

- **Smuggler results.** `DatabaseSmuggler` and its options are ported;
  `SmugglerResult` and `SmugglerProgressBase` are not, so
  `operation.fetch_operations_status()` hands back a raw dict rather than a typed result.
  That is where `SmugglerProgressBase.CdcSinksUpdated` would land.
  `OfflineMigration*` and `ShardedSmuggler*` follow features the client does not have.
- **`DatabaseSmugglerOptions.IsShard`.** `internal` with `[ForceJsonSerialization]`, so C#
  puts `"IsShard": false` on the wire. Omitted here; the server default matches.
- **`DatabaseItemType.Counters`.** Removed in C# 7.x, kept in the Python enum for callers
  written against older servers. It is not in the default selection.
