# Transaction State Machine and Replay Rules

## Write states

```text
prepared -> data-written -> parity-written -> metadata-written -> committed
          \-> aborted
          \-> replay-required
```

## Prototype implementation status

The current Go prototype now includes:

- append-only `journal.Record` entries with per-record checksums,
- durable `Store.Append()` writes with `fsync`,
- checksum-validated reloads via `Store.Load()`,
- replay summarization via `Store.Replay()` and `ReplayRecords()`,
- a journaled write coordinator that allocates extents and persists metadata,
- runnable demos with `go run ./cmd/rtpctl journal-demo` and `go run ./cmd/rtpctl write-demo`.

## Meaning of each state

| State | Meaning | Durable requirement |
| --- | --- | --- |
| `prepared` | Intent accepted and journaled | Journal fsync complete |
| `data-written` | New extent contents persisted | Data extent fsync complete |
| `parity-written` | Parity delta applied | Parity extent fsync complete |
| `metadata-written` | Metadata reflects new generation | Metadata transaction durable |
| `committed` | Safe to acknowledge to caller | Commit marker durable |
| `aborted` | Transaction rolled back or discarded | Abort marker durable |
| `replay-required` | Startup reconciliation must inspect the transaction | Recovery path entered |

## Crash matrix

| Crash point | Startup action | Expected result |
| --- | --- | --- |
| After `prepared`, before `data-written` | Discard pending write | No on-disk change becomes visible |
| After `data-written`, before `parity-written` | Recompute or roll forward parity using the journal payload | Data/parity re-synchronized |
| After `parity-written`, before `metadata-written` | Complete metadata update if generation/checksum matches | Metadata catches up to durable data |
| After `metadata-written`, before `committed` | Verify all checksums and then finalize commit during replay | Transaction becomes committed safely |
| Mid-record or checksum failure in journal | Stop at last valid record and mark pool degraded | Operator-visible replay warning |

## Replay algorithm

1. Open the journal and validate each record checksum.
2. Rebuild the set of transactions without a durable `committed` or `aborted` marker.
3. For each incomplete transaction:
   - verify the new data extent checksum,
   - verify or reconstruct parity,
   - compare metadata generation numbers,
   - roll forward when the durable data is trustworthy,
   - otherwise fall back to the previous generation and mark the write aborted.
4. Emit a replay report and clear the dirty flag only after all incomplete transactions are resolved.

## Daemon startup behavior

The prototype daemon now inspects the configured journal file during boot and exposes the result through:

- `GET /health`
- `GET /v1/journal`

If incomplete transactions are found, the daemon reports `status: "degraded"` and includes replay guidance in the returned summary.

## Operator rule

A pool is considered **clean** only when:

- the journal tail is fully checksummed,
- no transaction remains in `replay-required`, and
- a post-replay metadata snapshot has been written.
