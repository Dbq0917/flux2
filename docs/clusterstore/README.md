# Multi-cluster configuration store

## Background

Multi-cluster sync workers run concurrently and persist their shared
configuration to a single file. Before this package, each worker opened the
file and overwrote it in place. Two writers could interleave partial
content, leaving a file that no longer parsed. After a controller restart
the configuration could not be loaded, so every cluster sync stopped.

The `internal/clusterstore` package removes that failure mode while
keeping the existing configuration file path and YAML format unchanged.

## Guarantees

### Mutually exclusive, atomic writes

`Store.Save` serialises writers at two levels:

1. An in-process `sync.Mutex` serialises goroutines inside one controller
   process.
2. An advisory `flock(2)` lock on `<config-dir>/.cluster-config.lock`
   serialises writers across separate processes. The kernel releases the
   lock automatically when the holder exits, so a crashed process cannot
   deadlock the store.

The payload is written to a temporary file in the same directory, flushed
with `fsync`, and moved over the primary file with `rename(2)`, which is
atomic on POSIX filesystems. A reader therefore only ever observes the
previous complete file or the new complete file, never a torn write. The
parent directory is `fsync`-ed so the replacement is durable. On Windows
the same atomic-replacement guarantee is provided by `MoveFileEx`; see the
`lock_windows.go` limitation note for cross-process locking.

### Automatic rollback to the last good version

`Store.Load` first validates the primary file. When it is missing or
cannot be parsed and validated, the store walks
`<path>.bak.1`, `<path>.bak.2`, ... and restores the first parseable
backup atomically over the primary file. Corrupt backups are skipped and
recorded, not trusted. If neither the primary file nor any backup is
usable, `Load` returns `ErrUnrecoverable` (or `ErrNoConfiguration` when no
file exists at all) instead of silently starting empty.

### Failures are reported, never swallowed

Every I/O, locking, encoding or validation failure aborts the write and
is returned wrapped with `%w`. The previously committed primary file is
left untouched. Invalid configurations are rejected before any file
operation, so a bad payload cannot overwrite a good file.

### Stable path and format

The primary configuration remains a YAML document at the exact path
supplied by the caller (`Options.Path`), with the same schema. Backup,
lock and audit files use dedicated suffixes next to it and do not alter
the primary file's name or contents:

| File | Purpose |
| --- | --- |
| `<path>` | Primary configuration, path and YAML format owned by the caller |
| `<path>.bak.1` | Most recent previous good configuration |
| `<path>.bak.2` | Second most recent previous good configuration |
| `<path>.bak.3` | Third most recent previous good configuration |
| `<path>.audit.log` | Append-only JSON Lines audit trail |
| `<config-dir>/.cluster-config.lock` | Advisory cross-process lock |

## Backup strategy

- **Minimum retained versions: 3.** `DefaultBackupCount = 3` and any
  `Options.BackupCount` below 3 is clamped up to 3. Callers may retain more
  than 3, never fewer.
- `.bak.1` is always the newest previous good configuration, `.bak.N` the
  oldest within the retention window.
- A backup is only created from content that parses and validates. A
  corrupt primary file is never copied into the backup chain; the rotation
  step records a `config_corrupt` event and skips it.
- Backups are written with the same atomic temp-file-plus-rename path as
  the primary file, so a crash cannot leave a half-written backup.
- Files are created with mode `0600` and the configuration directory with
  mode `0700`. Kubeconfig material is referenced by path and never
  embedded, so no credential bytes are persisted.
- When the retention count is lowered, backups beyond the new limit are
  pruned defensively; they are never pruned during ordinary rotation.

## Audit trail

Every state-changing or recovery decision is appended as one JSON object
per line to `<path>.audit.log`, which survives process restarts. Event
types:

| Event | Meaning |
| --- | --- |
| `config_saved` | Primary file replaced atomically |
| `backup_rotated` | Previous primary file promoted to a numbered backup |
| `config_corrupt` | A candidate file failed parse/validation, with the reason |
| `config_recovered` | A backup was restored over the damaged primary file |
| `config_unrecoverable` | No candidate file was usable |

Each event carries a UTC timestamp, the affected path and a secret-free
detail string. The most recent 100 events are also retained in memory and
exposed through `Store.Events()` for programmatic review.

## Recovery runbook

Automatic recovery needs no operator action: the next `Store.Load` call
after corruption restores the newest good backup and records
`config_recovered`. For a manual review:

1. Inspect the audit trail:
   `cat <path>.audit.log | jq .`
2. Confirm which backup was selected and why earlier candidates were
   skipped.
3. Validate the restored primary file loads (the controller log and a
   `config_recovered` event confirm this).
4. If every file is corrupt (`config_unrecoverable`), restore from
   external configuration management or version control, then verify with
   a single controller restart.

The acceptance drill and its measured results are recorded in
[`recovery-drill.md`](recovery-drill.md), and the drill is re-runnable with
the `TestRecoveryDrill` unit test.
