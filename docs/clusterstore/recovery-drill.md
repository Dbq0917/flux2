# Recovery drill record: multi-cluster configuration store

This document is the reviewable record of the acceptance drill for the
concurrent-write corruption incident. The drill is encoded as
`TestRecoveryDrill` in `internal/clusterstore/recovery_drill_test.go` and
can be re-run at any time to reproduce every number below.

## Scope and acceptance criteria

| Criterion | How it is asserted |
| --- | --- |
| Two concurrent syncs must leave a parseable configuration file | Two goroutine writers perform 200 saves while an observer continuously parses the primary file; any unparseable observation fails the test |
| A deliberately corrupted file must roll back to the last good version | Primary file is overwritten with invalid YAML; `Load` must restore `clusters.yaml.bak.1` and return its content |
| Write failures must be reported | `TestSaveReportsWriteFailureAndKeepsPreviousFile` forces an I/O failure and asserts an error plus an untouched previous file |
| Existing path and format must not change | Primary file stays at the caller-supplied path; `TestSaveLoadRoundTrip` asserts the same YAML schema round-trips |
| Reviewable records must exist | Every decision is appended to `clusters.yaml.audit.log` and exposed via `Store.Events()`; assertions verify the events |

## Environment

| Item | Value |
| --- | --- |
| Date (UTC) | 2026-11-18 |
| Go toolchain | go1.27.0 linux/amd64 |
| Race detector | enabled (`-race`) |
| Package | `github.com/fluxcd/flux2/v2/internal/clusterstore` |

## Reproduction command

```sh
go test --tags=unit -race -count=1 -v \
  -run TestRecoveryDrill ./internal/clusterstore/
```

The test prints one `DRILL_RESULT` JSON object per phase to the test log.

## Measured results

### Phase 1: concurrent writes

Observed `DRILL_RESULT` (paths abbreviated):

```json
{
  "phase": "concurrent_writes",
  "writers": 2,
  "iterations": 200,
  "durationMs": 124,
  "primaryParseable": true,
  "tornWrites": 0,
  "backupsOnDisk": 3,
  "backupPaths": [
    "clusters.yaml.bak.1",
    "clusters.yaml.bak.2",
    "clusters.yaml.bak.3"
  ]
}
```

- 2 writers x 100 iterations = 200 committed saves, serialised by the
  in-process mutex and advisory lock.
- The observer parsed the primary file continuously and recorded
  `tornWrites: 0`. No interleaved or partial content was ever visible.
- All three retained backups remained present and independently parseable.

### Phase 2: corruption and rollback

Observed `DRILL_RESULT`:

```json
{
  "phase": "corruption_recovery",
  "primaryParseable": true,
  "backupsOnDisk": 3,
  "recoveredFrom": "clusters.yaml.bak.1",
  "recoveredCluster": "cluster-299"
}
```

The primary file was overwritten with
`clusters: [unterminated-sequence`, which fails YAML parsing. The next
`Load` call:

1. Recorded a `config_corrupt` event for the primary file with the parser
   reason.
2. Walked the backup chain and selected `clusters.yaml.bak.1`, the newest
   previous good version.
3. Restored it atomically over the primary file and recorded
   `config_recovered` with the source path.
4. Returned a valid configuration whose cluster name matched the backup
   content.

Audit excerpt (one JSON object per line in `clusters.yaml.audit.log`):

```json
{"type":"config_corrupt","path":".../clusters.yaml","detail":"failed to parse cluster config: error converting YAML to JSON: yaml: line 1: did not find expected ',' or ']'"}
{"type":"config_recovered","path":".../clusters.yaml.bak.1","detail":"restored backup version 1 over .../clusters.yaml"}
```

## Backup retention evidence

`TestBackupRetentionAndRotation` performs five saves against a retention
limit of three and asserts:

- `clusters.yaml.bak.1` through `clusters.yaml.bak.3` exist and parse.
- Their content is strictly newest-to-oldest (`cluster-4`, `cluster-3`,
  `cluster-2` after five saves).
- `clusters.yaml.bak.4` does not exist; anything beyond the limit is
  pruned.
- `TestBackupCountClampedToMinimum` asserts a requested limit of 1 is
  clamped to the required minimum of 3.

**Minimum retained versions: 3**, enforced by `DefaultBackupCount` in
`internal/clusterstore/store.go`.

## Full suite results

Command:

```sh
go test --tags=unit -race -count=1 -coverprofile=cover.out \
  ./internal/clusterstore/
```

Result: all 14 test functions pass under the race detector, statement
coverage 78.7%. The recovery path `recoverFromBackup` itself is at 90.0%
coverage; uncovered statements are platform-specific error branches and
the Windows lock stub.

Cross-compilation is verified with
`GOOS=windows GOARCH=amd64 go vet --tags=unit ./internal/clusterstore/`.

## Failure-mode evidence

| Test | Injected failure | Asserted behaviour |
| --- | --- | --- |
| `TestSaveReportsWriteFailureAndKeepsPreviousFile` | Configuration directory made read-only | `Save` returns a wrapped I/O error; previous primary file bytes are unchanged |
| `TestSaveRejectsInvalidConfig` | Empty cluster name | `Save` rejects before touching disk; no primary file is created |
| `TestRecoverWalksPastCorruptBackup` | Primary file and `.bak.1` corrupt | `.bak.2` is selected and restored; corrupt candidates are audited |
| `TestUnrecoverableWhenAllFilesCorrupt` | Primary file and all backups corrupt | `Load` returns `ErrUnrecoverable` and records `config_unrecoverable` |
| `TestSaveWaitsForExternalProcessLock` | Another process holds `flock` | `Save` blocks until the lock is released, then succeeds |

## Sign-off checklist

- [x] Concurrent two-writer drill leaves a parseable file with zero torn
      writes.
- [x] Manual corruption is recovered from `.bak.1` automatically.
- [x] At least three previous good versions are retained.
- [x] Write failures surface as errors and never destroy committed data.
- [x] Primary file path and YAML format are unchanged.
- [x] Audit trail is persisted across process restarts
      (`TestAuditLogPersistsAcrossReopen`).
- [x] Drill is re-runnable from a single `go test` command.
