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

// Package dependency resolves the dependency graph of Flux resources at
// parse time. It detects circular dependencies before reconciliation
// starts and drives a bounded retry queue so that a single misconfigured
// resource cannot starve the rest of the queue.
package dependency

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// Resource is the minimal view of a Flux object required for dependency
// resolution. It is intentionally decoupled from any controller API type
// so the parser keeps accepting the existing manifest format unchanged.
type Resource struct {
	// ID is the namespaced name of the resource.
	ID string
	// Kind is the API kind, e.g. Kustomization or HelmRelease.
	Kind string
	// Namespace of the resource.
	Namespace string
	// Name of the resource.
	Name string
	// DependsOn holds the resolved namespaced names this resource waits for.
	DependsOn []string
}

// Graph is a directed dependency graph. An edge dep -> dependent means the
// dependent cannot be reconciled until dep is ready.
type Graph struct {
	// Resources holds every parsed resource keyed by ID, including resources
	// that participate in a cycle.
	Resources map[string]*Resource
	// Edges maps a dependency ID to the IDs of resources depending on it.
	// Cyclic resources have no edges: they are failed at parse time.
	Edges map[string][]string
	// Cyclic holds the resources that participate in at least one cycle.
	Cyclic map[string]struct{}
	// Cycles holds one ordered node path per detected cycle.
	Cycles [][]string
}

// CycleError reports every circular dependency found while parsing a set of
// manifests. The error is returned at parse time, before any reconciliation
// runs, so a bad configuration never reaches the runtime queue.
type CycleError struct {
	Cycles [][]string
}

// Error implements error.
func (e *CycleError) Error() string {
	descriptions := make([]string, 0, len(e.Cycles))
	for _, cycle := range e.Cycles {
		descriptions = append(descriptions, strings.Join(cycle, " -> "))
	}
	return fmt.Sprintf("circular dependency detected: %s", strings.Join(descriptions, "; "))
}

// MissingDependencyError references a resource whose dependsOn entry cannot
// be resolved to a parsed object.
type MissingDependencyError struct {
	ResourceID   string
	DependencyID string
}

// Error implements error.
func (e *MissingDependencyError) Error() string {
	return fmt.Sprintf("resource %q depends on unknown resource %q", e.ResourceID, e.DependencyID)
}

// BuildGraph constructs the dependency graph for the given resources. It
// validates references and runs cycle detection during parsing. When cycles
// or dangling references are present, the returned graph is still populated
// (callers can inspect Graph.Cyclic and reconcile the unaffected resources)
// together with an error joining every detected problem.
func BuildGraph(resources []*Resource) (*Graph, error) {
	graph := &Graph{
		Resources: make(map[string]*Resource, len(resources)),
		Edges:     make(map[string][]string),
		Cyclic:    make(map[string]struct{}),
	}

	for _, resource := range resources {
		if resource.ID == "" {
			return nil, errors.New("resource with empty id is not allowed")
		}
		if _, exists := graph.Resources[resource.ID]; exists {
			return nil, fmt.Errorf("duplicate resource id %q", resource.ID)
		}
		graph.Resources[resource.ID] = resource
	}

	var errs []error
	for _, resource := range resources {
		for _, dep := range resource.DependsOn {
			if _, exists := graph.Resources[dep]; !exists {
				errs = append(errs, &MissingDependencyError{
					ResourceID:   resource.ID,
					DependencyID: dep,
				})
			}
		}
	}

	graph.Cycles = detectCycles(resources)
	if len(graph.Cycles) > 0 {
		for _, cycle := range graph.Cycles {
			for _, id := range cycle {
				graph.Cyclic[id] = struct{}{}
			}
		}
		errs = append(errs, &CycleError{Cycles: graph.Cycles})
	}

	for _, resource := range resources {
		if _, cyclic := graph.Cyclic[resource.ID]; cyclic {
			continue
		}
		for _, dep := range resource.DependsOn {
			if _, cyclic := graph.Cyclic[dep]; cyclic {
				continue
			}
			if _, exists := graph.Resources[dep]; !exists {
				continue
			}
			graph.Edges[dep] = append(graph.Edges[dep], resource.ID)
		}
	}
	for dep := range graph.Edges {
		sort.Strings(graph.Edges[dep])
	}

	return graph, errors.Join(errs...)
}

// detectCycles runs an iterative depth-first search over the dependsOn
// edges and returns one node path per distinct cycle, each closed with the
// starting node repeated at the end.
func detectCycles(resources []*Resource) [][]string {
	const (
		unvisited = 0
		inStack   = 1
		done      = 2
	)

	color := make(map[string]int, len(resources))
	byID := make(map[string]*Resource, len(resources))
	stack := make([]string, 0, len(resources))
	seenCycle := make(map[string]struct{})
	var cycles [][]string

	ids := make([]string, 0, len(resources))
	for _, resource := range resources {
		ids = append(ids, resource.ID)
		byID[resource.ID] = resource
	}
	sort.Strings(ids)

	var visit func(id string)
	visit = func(id string) {
		color[id] = inStack
		stack = append(stack, id)

		resource, ok := byID[id]
		if ok {
			deps := append([]string(nil), resource.DependsOn...)
			sort.Strings(deps)
			for _, dep := range deps {
				if _, exists := byID[dep]; !exists {
					continue
				}
				switch color[dep] {
				case unvisited:
					visit(dep)
				case inStack:
					cycle := extractCycle(stack, dep)
					key := strings.Join(cycle, "|")
					if _, recorded := seenCycle[key]; !recorded {
						seenCycle[key] = struct{}{}
						cycles = append(cycles, cycle)
					}
				}
			}
		}

		stack = stack[:len(stack)-1]
		color[id] = done
	}

	for _, id := range ids {
		if color[id] == unvisited {
			visit(id)
		}
	}
	return cycles
}

// extractCycle returns the stack suffix starting at the repeated node and
// appends the node again to render a closed path a -> b -> a.
func extractCycle(stack []string, repeated string) []string {
	start := 0
	for index, id := range stack {
		if id == repeated {
			start = index
			break
		}
	}
	cycle := append([]string(nil), stack[start:]...)
	return append(cycle, repeated)
}

// Schedulable returns the IDs that are not part of a detected cycle.
func (g *Graph) Schedulable() []string {
	ids := make([]string, 0, len(g.Resources))
	for id := range g.Resources {
		if _, cyclic := g.Cyclic[id]; !cyclic {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

// TopologicalOrder returns the reconciliation order for the non-cyclic
// resources: dependencies always precede their dependents.
func (g *Graph) TopologicalOrder() ([]string, error) {
	indegree := make(map[string]int, len(g.Resources))
	for id := range g.Resources {
		if _, cyclic := g.Cyclic[id]; cyclic {
			continue
		}
		indegree[id] = 0
	}
	for dep, dependents := range g.Edges {
		if _, cyclic := g.Cyclic[dep]; cyclic {
			continue
		}
		for _, dependent := range dependents {
			indegree[dependent]++
		}
	}

	ready := make([]string, 0)
	for id, degree := range indegree {
		if degree == 0 {
			ready = append(ready, id)
		}
	}
	sort.Strings(ready)

	order := make([]string, 0, len(indegree))
	for len(ready) > 0 {
		id := ready[0]
		ready = ready[1:]
		order = append(order, id)

		next := append([]string(nil), g.Edges[id]...)
		sort.Strings(next)
		for _, dependent := range next {
			indegree[dependent]--
			if indegree[dependent] == 0 {
				ready = append(ready, dependent)
			}
		}
	}

	if len(order) != len(indegree) {
		return nil, errors.New("topological sort failed: residual cycle in graph")
	}
	return order, nil
}
