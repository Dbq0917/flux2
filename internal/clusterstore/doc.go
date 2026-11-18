/*
Copyright 2026 The Flux authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package clusterstore persists the multi-cluster configuration used by
// concurrent cluster sync loops.
//
// A single configuration file is shared by every sync worker. To prevent
// concurrent writers from interleaving partial content and corrupting the
// file, every write is serialised with an in-process mutex and an advisory
// file lock, then published atomically with rename(2). Before each overwrite
// the previous configuration is rotated into numbered backup files, and Load
// automatically falls back to the most recent parseable backup when the
// primary file is damaged. Every recovery and rotation decision is recorded
// in an append-only audit log so that post-incident reviews can reconstruct
// what happened.
//
// The on-disk path and YAML schema of the primary configuration file are
// owned by the caller and are not changed by this package; backup and lock
// files are created next to the primary file with dedicated suffixes.
package clusterstore
