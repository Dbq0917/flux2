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

package clusterstore

import (
	"fmt"
	"os"
	"path/filepath"
)

// defaultFileMode is the mode used for configuration, backup, lock and
// audit files. Configuration may reference kubeconfig paths but never
// embeds credentials, so 0600 still avoids exposing cluster topology to
// other local users.
const defaultFileMode os.FileMode = 0o600

// writeFileAtomic writes data to path without ever exposing a partially
// written file at that path. The content is first written to a temporary
// file in the same directory (rename(2) only works across the same
// filesystem), flushed to disk, then moved into place atomically. The
// temporary file is removed on every error path.
func writeFileAtomic(dir, path string, data []byte, mode os.FileMode) (retErr error) {
	tmp, err := os.CreateTemp(dir, ".cluster-config-*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temporary file for %s: %w", path, err)
	}
	tmpName := tmp.Name()
	defer func() {
		if retErr != nil {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("failed to write temporary file for %s: %w", path, err)
	}
	if err := syncFile(tmp); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("failed to flush temporary file for %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("failed to close temporary file for %s: %w", path, err)
	}
	if err := os.Chmod(tmpName, mode); err != nil {
		return fmt.Errorf("failed to set permissions on temporary file for %s: %w", path, err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("failed to atomically replace %s: %w", path, err)
	}
	if err := syncDir(dir); err != nil {
		return fmt.Errorf("failed to flush directory entry for %s: %w", path, err)
	}
	return nil
}

// copyFile copies the regular file at src to dst atomically, preserving
// the given mode. It is used to rotate backups so a crash never leaves a
// half-written backup behind.
func copyFileAtomic(src, dst string, mode os.FileMode) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return fmt.Errorf("failed to read %s for backup: %w", src, err)
	}
	return writeFileAtomic(filepath.Dir(dst), dst, data, mode)
}
