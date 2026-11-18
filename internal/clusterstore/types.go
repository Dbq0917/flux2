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
	"errors"
	"fmt"

	"sigs.k8s.io/yaml"
)

// ConfigAPIVersion is the schema version stamped on every persisted
// Config. It is additive only: older files without the stamp remain
// readable and unknown fields are preserved by the YAML decoder.
const ConfigAPIVersion = "fluxcd.io/v1"

// ConfigKind identifies a persisted multi-cluster configuration document.
const ConfigKind = "ClusterConfig"

// Cluster describes a single member cluster reachable during sync.
type Cluster struct {
	// Name is the unique identifier of the cluster inside the configuration.
	Name string `json:"name"`
	// Endpoint is the Kubernetes API server endpoint of the cluster.
	Endpoint string `json:"endpoint"`
	// KubeConfigPath is the path to the kubeconfig credential for the cluster.
	// Credential material is referenced, never embedded.
	KubeConfigPath string `json:"kubeConfigPath,omitempty"`
	// Enabled toggles whether sync workers reconcile the cluster.
	Enabled bool `json:"enabled,omitempty"`
}

// Config is the on-disk multi-cluster configuration document. Its YAML
// representation is the persisted file format and must stay backward
// compatible: fields may only be added additively.
type Config struct {
	APIVersion string    `json:"apiVersion"`
	Kind       string    `json:"kind"`
	Clusters   []Cluster `json:"clusters"`
}

// Validate rejects configurations that cannot be used safely by a sync
// worker. A document that fails validation is treated as corrupt and
// triggers backup recovery on load.
func (c *Config) Validate() error {
	if c == nil {
		return errors.New("cluster config is nil")
	}
	if c.APIVersion != "" && c.APIVersion != ConfigAPIVersion {
		return fmt.Errorf("unsupported cluster config apiVersion %q", c.APIVersion)
	}
	if c.Kind != "" && c.Kind != ConfigKind {
		return fmt.Errorf("unsupported cluster config kind %q", c.Kind)
	}
	seen := make(map[string]struct{}, len(c.Clusters))
	for _, cluster := range c.Clusters {
		if cluster.Name == "" {
			return errors.New("cluster entry has an empty name")
		}
		if cluster.Endpoint == "" {
			return fmt.Errorf("cluster %q has an empty endpoint", cluster.Name)
		}
		if _, ok := seen[cluster.Name]; ok {
			return fmt.Errorf("duplicate cluster name %q", cluster.Name)
		}
		seen[cluster.Name] = struct{}{}
	}
	return nil
}

// marshalConfig renders the configuration in the canonical YAML format used
// for the primary file and every backup.
func marshalConfig(config *Config) ([]byte, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("refusing to persist invalid cluster config: %w", err)
	}
	stamped := *config
	stamped.APIVersion = ConfigAPIVersion
	stamped.Kind = ConfigKind
	data, err := yaml.Marshal(&stamped)
	if err != nil {
		return nil, fmt.Errorf("failed to encode cluster config: %w", err)
	}
	return data, nil
}

// unmarshalConfig parses and validates a configuration document.
func unmarshalConfig(data []byte) (*Config, error) {
	if len(data) == 0 {
		return nil, errors.New("cluster config file is empty")
	}
	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse cluster config: %w", err)
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &config, nil
}
