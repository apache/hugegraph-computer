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
	"strings"
	"testing"
	"vermeer/apps/common"
	"vermeer/apps/serialize"
)

func TestValidateCycleSearchConfig(t *testing.T) {
	tests := []struct {
		name      string
		mode      cyclesMode
		minLength int
		maxLength int
		limit     int
		wantError string
	}{
		{name: "max_too_small", mode: All, maxLength: 1, wantError: "max_length"},
		{name: "negative_min", mode: All, minLength: -1, maxLength: 5, wantError: "min_length"},
		{name: "min_exceeds_max", mode: All, minLength: 6, maxLength: 5, wantError: "exceeds"},
		{name: "limit_requires_positive_cap", mode: Limit, minLength: 2, maxLength: 5, wantError: "max_cycles"},
		{name: "invalid_mode", mode: cyclesMode("unknown"), minLength: 2, maxLength: 5, wantError: "mode"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := validateCycleSearchConfig(test.mode, test.minLength,
				test.maxLength, test.limit)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("expected error containing %q, got %v", test.wantError, err)
			}
		})
	}

	config, err := validateCycleSearchConfig(All, 0, 5, 0)
	if err != nil {
		t.Fatalf("default all-mode options rejected: %v", err)
	}
	if config.minLength != 2 || config.maxLength != 5 || config.limit != 0 {
		t.Fatalf("unexpected normalized all-mode config: %+v", config)
	}

	config, err = validateCycleSearchConfig(Boolean, 3, 4, 0)
	if err != nil {
		t.Fatalf("boolean options rejected: %v", err)
	}
	if config.limit != 1 {
		t.Fatalf("boolean mode must stop after the first cycle, got limit %d", config.limit)
	}
}

func TestSearchBoundedCyclesUsesActualCycleLength(t *testing.T) {
	// Incoming-edge adjacency for directed cycle 0 -> 1 -> 2 -> 0.
	adjacency := []serialize.SliceUint32{{2}, {0}, {1}}
	config := cycleSearchConfig{minLength: 3, maxLength: 3}
	var cycles [][]serialize.SUint32
	count := searchBoundedCycles(0, adjacency, config, nil,
		func(cycle []serialize.SUint32) { cycles = append(cycles, cycle) })
	if count != 1 || len(cycles) != 1 {
		t.Fatalf("expected one three-cycle, got count=%d cycles=%v", count, cycles)
	}
	if len(cycles[0]) != 2 || cycles[0][0] != 2 || cycles[0][1] != 1 {
		t.Fatalf("root-implied cycle output changed unexpectedly: %v", cycles[0])
	}

	config = cycleSearchConfig{minLength: 2, maxLength: 2}
	if got := searchBoundedCycles(0, adjacency, config, nil,
		func([]serialize.SUint32) {}); got != 0 {
		t.Fatalf("three-cycle incorrectly passed max_length=2: %d", got)
	}
}

func TestSearchBoundedCyclesSupportsTwoCycles(t *testing.T) {
	adjacency := []serialize.SliceUint32{{1}, {0}}
	config := cycleSearchConfig{minLength: 2, maxLength: 2}
	var cycle []serialize.SUint32
	count := searchBoundedCycles(0, adjacency, config, nil,
		func(found []serialize.SUint32) { cycle = found })
	if count != 1 || len(cycle) != 1 || cycle[0] != 1 {
		t.Fatalf("expected one two-cycle rooted at 0, got count=%d cycle=%v", count, cycle)
	}
}

func TestSearchBoundedCyclesStopsAtLimit(t *testing.T) {
	adjacency := []serialize.SliceUint32{{1, 2}, {0}, {0}}
	config := cycleSearchConfig{minLength: 2, maxLength: 3, limit: 1}
	emitted := 0
	count := searchBoundedCycles(0, adjacency, config, nil,
		func([]serialize.SUint32) { emitted++ })
	if count != 1 || emitted != 1 {
		t.Fatalf("limit mode emitted too many cycles: count=%d emitted=%d", count, emitted)
	}
}

func TestSearchBoundedCyclesRejectsRepeatedVertices(t *testing.T) {
	// 0 -> 1 -> 2 -> 1 does not return to root 0.
	adjacency := []serialize.SliceUint32{{}, {0, 2}, {1}}
	config := cycleSearchConfig{minLength: 2, maxLength: 6}
	if got := searchBoundedCycles(0, adjacency, config, nil,
		func([]serialize.SUint32) {}); got != 0 {
		t.Fatalf("non-simple walk was reported as a root cycle: %d", got)
	}
}

func TestSearchBoundedCyclesCanonicalFilterAvoidsDuplicateRoots(t *testing.T) {
	adjacency := []serialize.SliceUint32{{2}, {0}, {1}}
	config := cycleSearchConfig{minLength: 3, maxLength: 3}
	allowGreaterThanRoot := func(root serialize.SUint32) func(serialize.SUint32) bool {
		return func(next serialize.SUint32) bool { return next > root }
	}
	if got := searchBoundedCycles(0, adjacency, config, allowGreaterThanRoot(0),
		func([]serialize.SUint32) {}); got != 1 {
		t.Fatalf("minimum root should own the cycle, got %d", got)
	}
	if got := searchBoundedCycles(1, adjacency, config, allowGreaterThanRoot(1),
		func([]serialize.SUint32) {}); got != 0 {
		t.Fatalf("non-minimum root duplicated the cycle, got %d", got)
	}
}

func TestSearchBoundedCyclesSkipsInvalidRemoteIDs(t *testing.T) {
	adjacency := []serialize.SliceUint32{{1, 99}, {0}}
	config := cycleSearchConfig{minLength: 2, maxLength: 2}
	if got := searchBoundedCycles(0, adjacency, config, nil,
		func([]serialize.SUint32) {}); got != 1 {
		t.Fatalf("invalid remote ID changed valid cycle count: %d", got)
	}
}

func TestCycleDetectionOutputTypeMatchesMode(t *testing.T) {
	worker := &CycleDetectionWorker{mode: Boolean}
	if got := worker.OutputValueType(); got != common.HgValueTypeInt {
		t.Fatalf("boolean cycle output must be INT, got %q", got)
	}
	worker.mode = All
	if got := worker.OutputValueType(); got != common.HgValueTypeString {
		t.Fatalf("cycle-list output must be TEXT, got %q", got)
	}
}
