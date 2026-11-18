/*
Copyright 2026 The Flux Authors

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
	"errors"
	"os"
	"strings"
	"testing"
)

var errFlaky = errors.New("simulated reconcile failure")

func openTestFile(t *testing.T, path string) *os.File {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("failed to open %s: %v", path, err)
	}
	t.Cleanup(func() { _ = file.Close() })
	return file
}

func asError(err error, target **CycleError) bool {
	return errors.As(err, target)
}

func TestBuildGraphDetectsCycleAtParseTime(t *testing.T) {
	resources := []*Resource{
		{ID: "a", DependsOn: []string{"b"}},
		{ID: "b", DependsOn: []string{"c"}},
		{ID: "c", DependsOn: []string{"a"}},
	}

	graph, err := BuildGraph(resources)
	if err == nil {
		t.Fatal("expected cycle error")
	}
	var cycleErr *CycleError
	if !errors.As(err, &cycleErr) {
		t.Fatalf("expected *CycleError, got %T", err)
	}
	if len(cycleErr.Cycles) != 1 {
		t.Fatalf("expected one cycle, got %d: %v", len(cycleErr.Cycles), cycleErr.Cycles)
	}
	cycle := cycleErr.Cycles[0]
	if cycle[0] != cycle[len(cycle)-1] {
		t.Errorf("cycle path must be closed, got %v", cycle)
	}
	if len(graph.Cyclic) != 3 {
		t.Errorf("expected all three nodes marked cyclic, got %v", graph.Cyclic)
	}
	if !strings.Contains(err.Error(), "circular dependency detected") {
		t.Errorf("error message should mention circular dependency, got %q", err.Error())
	}
}

func TestBuildGraphSelfReferenceIsACycle(t *testing.T) {
	_, err := BuildGraph([]*Resource{{ID: "a", DependsOn: []string{"a"}}})
	if err == nil {
		t.Fatal("expected self-reference to be reported as a cycle")
	}
	var cycleErr *CycleError
	if !errors.As(err, &cycleErr) {
		t.Fatalf("expected *CycleError, got %T", err)
	}
}

func TestBuildGraphRejectsMissingDependency(t *testing.T) {
	_, err := BuildGraph([]*Resource{
		{ID: "a", DependsOn: []string{"missing"}},
	})
	var missingErr *MissingDependencyError
	if !errors.As(err, &missingErr) {
		t.Fatalf("expected *MissingDependencyError, got %T: %v", err, err)
	}
	if missingErr.ResourceID != "a" || missingErr.DependencyID != "missing" {
		t.Errorf("unexpected error fields: %+v", missingErr)
	}
}

func TestBuildGraphRejectsDuplicateID(t *testing.T) {
	_, err := BuildGraph([]*Resource{
		{ID: "a"},
		{ID: "a"},
	})
	if err == nil || !strings.Contains(err.Error(), `duplicate resource id "a"`) {
		t.Fatalf("expected duplicate id error, got %v", err)
	}
}

func TestTopologicalOrderRespectsDependencies(t *testing.T) {
	resources := []*Resource{
		{ID: "a"},
		{ID: "b", DependsOn: []string{"a"}},
		{ID: "c", DependsOn: []string{"a"}},
		{ID: "d", DependsOn: []string{"b", "c"}},
	}
	graph, err := BuildGraph(resources)
	if err != nil {
		t.Fatalf("BuildGraph returned error: %v", err)
	}

	order, err := graph.TopologicalOrder()
	if err != nil {
		t.Fatalf("TopologicalOrder returned error: %v", err)
	}
	position := map[string]int{}
	for index, id := range order {
		position[id] = index
	}
	if position["a"] > position["b"] || position["a"] > position["c"] {
		t.Errorf("a must precede b and c, got %v", order)
	}
	if position["b"] > position["d"] || position["c"] > position["d"] {
		t.Errorf("b and c must precede d, got %v", order)
	}
}

func TestParseManifestsKeepsExistingDependsOnFormat(t *testing.T) {
	file := openTestFile(t, "testdata/cyclic-manifests.yaml")
	resources, err := ParseManifests(file)
	if err != nil {
		t.Fatalf("ParseManifests returned error: %v", err)
	}
	if len(resources) != 4 {
		t.Fatalf("expected 4 resources, got %d", len(resources))
	}

	byID := map[string]*Resource{}
	for _, resource := range resources {
		byID[resource.ID] = resource
	}
	apps := byID["flux-system/apps"]
	if apps == nil || len(apps.DependsOn) != 1 || apps.DependsOn[0] != "flux-system/infra" {
		t.Errorf("unexpected apps dependencies: %+v", apps)
	}
	if byID["flux-system/independent"].Kind != "Kustomization" {
		t.Error("expected independent Kustomization to be parsed")
	}
}

func TestParseManifestsCrossNamespaceReference(t *testing.T) {
	manifest := strings.NewReader(`
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: app
  namespace: team-a
spec:
  dependsOn:
    - name: shared
      namespace: team-b
`)
	resources, err := ParseManifests(manifest)
	if err != nil {
		t.Fatalf("ParseManifests returned error: %v", err)
	}
	if len(resources) != 1 {
		t.Fatalf("expected one resource, got %d", len(resources))
	}
	if got := resources[0].DependsOn[0]; got != "team-b/shared" {
		t.Errorf("cross-namespace dependency = %q, want team-b/shared", got)
	}
}
