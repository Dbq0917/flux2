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

package dependency

import (
	"fmt"
	"io"

	"k8s.io/apimachinery/pkg/util/yaml"
)

// dependencyReference mirrors the existing spec.dependsOn entry used by
// Kustomization and HelmRelease manifests. The on-disk format is unchanged.
type dependencyReference struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

type manifestSpec struct {
	DependsOn []dependencyReference `json:"dependsOn"`
}

type manifestObject struct {
	Kind       string `json:"kind"`
	APIVersion string `json:"apiVersion"`
	Metadata   struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	} `json:"metadata"`
	Spec manifestSpec `json:"spec"`
}

// ParseManifests decodes a multi-document YAML stream and extracts the
// dependency view of every Kustomization and HelmRelease object. Unknown
// kinds are ignored, matching how a controller only consumes its own CRDs.
// The existing manifest format, including cross-namespace dependsOn
// references, is accepted without modification.
func ParseManifests(reader io.Reader) ([]*Resource, error) {
	decoder := yaml.NewYAMLOrJSONDecoder(reader, 4096)

	var resources []*Resource
	documentIndex := -1
	for {
		documentIndex++
		var object manifestObject
		if err := decoder.Decode(&object); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to decode manifest document %d: %w", documentIndex, err)
		}
		if object.Kind == "" || object.Metadata.Name == "" {
			continue
		}
		if object.Kind != "Kustomization" && object.Kind != "HelmRelease" {
			continue
		}

		namespace := object.Metadata.Namespace
		resource := &Resource{
			Kind:      object.Kind,
			Namespace: namespace,
			Name:      object.Metadata.Name,
			ID:        objectID(namespace, object.Metadata.Name),
		}
		for _, reference := range object.Spec.DependsOn {
			depNamespace := reference.Namespace
			if depNamespace == "" {
				depNamespace = namespace
			}
			resource.DependsOn = append(resource.DependsOn, objectID(depNamespace, reference.Name))
		}
		resources = append(resources, resource)
	}
	return resources, nil
}

func objectID(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + "/" + name
}
