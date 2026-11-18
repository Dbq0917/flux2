//go:build windows

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

import "os"

// Windows does not offer flock(2); the in-process mutex still serialises
// goroutine writers, and atomic replacement prevents torn writes. Opening
// the lock file keeps the on-disk layout identical across platforms. A
// future revision can back this with LockFileEx for multi-process support.
type fileLock struct {
	handle *os.File
}

func acquireFileLock(path string) (*fileLock, error) {
	handle, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, defaultFileMode)
	if err != nil {
		return nil, err
	}
	return &fileLock{handle: handle}, nil
}

func (l *fileLock) release() error {
	if l == nil || l.handle == nil {
		return nil
	}
	err := l.handle.Close()
	l.handle = nil
	return err
}
