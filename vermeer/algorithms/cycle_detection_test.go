/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with this
work for additional information regarding copyright ownership. The ASF
licenses this file to You under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations
under the License.
*/

package algorithms

import (
	"reflect"
	"testing"
	"vermeer/apps/common"
	"vermeer/apps/options"
	"vermeer/apps/serialize"
	"vermeer/apps/structure"
)

func init() {
	options.Init()
}

func TestParseCycleParameters(t *testing.T) {
	parameters, err := parseCycleParameters(map[string]string{
		"compute.max_step": "2",
		"cycle.mode":       "all",
		"cycle.min_length": "0",
		"cycle.max_length": "10",
		"cycle.max_cycles": "10",
	}, 4)
	if err != nil {
		t.Fatalf("valid parameters rejected: %v", err)
	}
	if parameters.minLen != 2 || parameters.maxLen != 4 {
		t.Fatalf("unexpected normalized bounds: %+v", parameters)
	}

	tests := []struct {
		name   string
		params map[string]string
	}{
		{
			name: "short_compute_budget",
			params: map[string]string{
				"compute.max_step": "1", "cycle.mode": "all",
				"cycle.min_length": "2", "cycle.max_length": "3",
			},
		},
		{
			name: "max_below_two",
			params: map[string]string{
				"compute.max_step": "2", "cycle.mode": "all",
				"cycle.min_length": "0", "cycle.max_length": "1",
			},
		},
		{
			name: "min_above_max",
			params: map[string]string{
				"compute.max_step": "2", "cycle.mode": "all",
				"cycle.min_length": "4", "cycle.max_length": "3",
			},
		},
		{
			name: "zero_limit",
			params: map[string]string{
				"compute.max_step": "2", "cycle.mode": "limit",
				"cycle.min_length": "2", "cycle.max_length": "3",
				"cycle.max_cycles": "0",
			},
		},
		{
			name: "unknown_mode",
			params: map[string]string{
				"compute.max_step": "2", "cycle.mode": "unknown",
				"cycle.min_length": "2", "cycle.max_length": "3",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parseCycleParameters(test.params, 10); err == nil {
				t.Fatal("invalid parameters were accepted")
			}
		})
	}
}

func TestBuildCycleRanksUsesExternalVertexIDs(t *testing.T) {
	ranks := buildCycleRanks([]string{"2", "10", "1"})
	want := []uint32{2, 1, 0}
	if !reflect.DeepEqual(ranks, want) {
		t.Fatalf("expected lexical external-ID ranks %v, got %v", want, ranks)
	}
}

func TestSortedUniqueCycleNeighbors(t *testing.T) {
	got := sortedUniqueCycleNeighbors(serialize.SliceUint32{3, 1, 3, 2, 1})
	want := serialize.SliceUint32{1, 2, 3}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestBoundedCycleSearchUsesActualCycleLength(t *testing.T) {
	adjacency := directedTriangleAdjacency()
	ranks := []uint32{0, 1, 2}
	state := newCycleSearchState(3)

	cycles, found := boundedCycleSearch(0, adjacency, ranks, &state,
		cycleSearchOptions{
			minLen: 3, maxLen: 3, canonical: true, collect: true,
		})
	if found != 1 || len(cycles) != 1 ||
		!reflect.DeepEqual(cycles[0], []uint32{2, 1}) {
		t.Fatalf("expected one length-3 cycle, found=%d cycles=%v", found, cycles)
	}

	_, found = boundedCycleSearch(0, adjacency, ranks, &state,
		cycleSearchOptions{
			minLen: 2, maxLen: 2, canonical: true, collect: true,
		})
	if found != 0 {
		t.Fatalf("length-3 cycle passed max_length=2: %d", found)
	}

	_, found = boundedCycleSearch(1, adjacency, ranks, &state,
		cycleSearchOptions{
			minLen: 3, maxLen: 3, canonical: true, collect: true,
		})
	if found != 0 {
		t.Fatalf("non-minimum canonical root emitted the same cycle: %d", found)
	}
}

func TestBooleanCycleSearchFindsEveryCycleVertex(t *testing.T) {
	adjacency := directedTriangleAdjacency()
	ranks := []uint32{0, 1, 2}
	state := newCycleSearchState(3)
	for rootID := uint32(0); rootID < 3; rootID++ {
		cycles, found := boundedCycleSearch(rootID, adjacency, ranks, &state,
			cycleSearchOptions{
				minLen: 3, maxLen: 3, maxResults: 1,
				canonical: false, collect: false,
			})
		if found != 1 || len(cycles) != 0 {
			t.Fatalf("root %d: expected boolean short-circuit, found=%d cycles=%v",
				rootID, found, cycles)
		}
	}
}

func TestLimitModeStopsAtRequestedResultCount(t *testing.T) {
	adjacency := []serialize.SliceUint32{
		{1, 2, 3},
		{0, 2, 3},
		{0, 1, 3},
		{0, 1, 2},
	}
	ranks := []uint32{0, 1, 2, 3}
	state := newCycleSearchState(4)
	cycles, found := boundedCycleSearch(0, adjacency, ranks, &state,
		cycleSearchOptions{
			minLen: 2, maxLen: 4, maxResults: 1,
			canonical: true, collect: true,
		})
	if found != 1 || len(cycles) != 1 {
		t.Fatalf("expected exactly one collected cycle, found=%d cycles=%v",
			found, cycles)
	}

	// Early return leaves internal slices populated, so run again to verify reset
	// safely reuses the same per-partition state.
	cycles, found = boundedCycleSearch(0, adjacency, ranks, &state,
		cycleSearchOptions{
			minLen: 2, maxLen: 4, maxResults: 1,
			canonical: true, collect: true,
		})
	if found != 1 || len(cycles) != 1 {
		t.Fatalf("reused state changed result, found=%d cycles=%v", found, cycles)
	}
}

func TestCycleWorkerLifecycleAndCanonicalOutput(t *testing.T) {
	worker := newCycleTestWorker(t, All)
	for step := int32(1); step <= 2; step++ {
		worker.WContext.Step = step
		for vertexID := uint32(0); vertexID < 3; vertexID++ {
			worker.Compute(vertexID, 0)
		}
		worker.AfterStep()
	}

	// External IDs are ["2", "10", "1"], so global vertex 2 owns the cycle.
	if len(worker.cycleList[0]) != 0 || len(worker.cycleList[1]) != 0 ||
		len(worker.cycleList[2]) != 1 {
		t.Fatalf("unexpected canonical cycle ownership: %v", worker.cycleList)
	}
	if count := worker.WContext.GetValue(cycleCountValue).(serialize.SInt64); count != 1 {
		t.Fatalf("expected one aggregated local cycle, got %d", count)
	}
}

func TestCycleBooleanLifecycleAndOutputType(t *testing.T) {
	worker := newCycleTestWorker(t, Boolean)
	for step := int32(1); step <= 2; step++ {
		worker.WContext.Step = step
		for vertexID := uint32(0); vertexID < 3; vertexID++ {
			worker.Compute(vertexID, 0)
		}
		worker.AfterStep()
	}
	for vertexID, value := range worker.hasCycle {
		if value != 1 {
			t.Fatalf("vertex %d was not marked as cyclic: %v", vertexID, worker.hasCycle)
		}
	}
	if worker.OutputValueType() != common.HgValueTypeInt {
		t.Fatalf("boolean mode output type is %q", worker.OutputValueType())
	}

	worker.parameters.mode = All
	if worker.OutputValueType() != common.HgValueTypeString {
		t.Fatalf("list mode output type is %q", worker.OutputValueType())
	}
}

func TestCycleStepTwoScatterPayloadIsEmpty(t *testing.T) {
	worker := &CycleDetectionWorker{}
	context := worker.MakeContext(nil)
	context.Step = 2
	if size := worker.VertexValue(0).PredictSize(); size != 0 {
		t.Fatalf("step-two scatter value predicts %d bytes, expected 0", size)
	}
}

func directedTriangleAdjacency() []serialize.SliceUint32 {
	// Reversed traversal adjacency: 0 <- 2 <- 1 <- 0.
	return []serialize.SliceUint32{{2}, {0}, {1}}
}

func newCycleTestWorker(t *testing.T, mode cyclesMode) *CycleDetectionWorker {
	t.Helper()
	graph := &structure.GraphData{
		VertexCount: 3,
		Vertex: &structure.VertexMem{
			TotalVertex: []structure.Vertex{
				{ID: "2"}, {ID: "10"}, {ID: "1"},
			},
		},
		Edges: &structure.EdgeMem{
			InEdges: serialize.TwoDimSliceUint32{{2}, {0}, {1}},
		},
	}
	worker := &CycleDetectionWorker{}
	context := worker.MakeContext(map[string]string{
		"compute.max_step": "2",
		"cycle.mode":       string(mode),
		"cycle.min_length": "3",
		"cycle.max_length": "3",
		"cycle.max_cycles": "1",
	})
	context.GraphData = graph
	context.Parallel = 1
	if err := worker.Init(); err != nil {
		t.Fatalf("cycle worker init: %v", err)
	}
	return worker
}
