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
	"testing"
	"vermeer/apps/serialize"
)

func TestParseCycleParametersRejectsNegativeMinimum(t *testing.T) {
	_, err := parseCycleParameters(map[string]string{
		"compute.max_step": "2",
		"cycle.mode":       "all",
		"cycle.min_length": "-1",
		"cycle.max_length": "3",
	}, 3)
	if err == nil {
		t.Fatal("negative cycle.min_length was accepted")
	}
}

func TestBoundedCycleSearchPreventsRepeatedVertices(t *testing.T) {
	// The only walk from 0 is 0 -> 1 -> 2 -> 1. It repeats vertex 1 and never
	// returns to the root, so it must not be reported as a simple cycle.
	adjacency := []serialize.SliceUint32{{1}, {2}, {1}}
	state := newCycleSearchState(6)
	cycles, found := boundedCycleSearch(0, adjacency, nil, &state,
		cycleSearchOptions{
			minLen: 2, maxLen: 6, canonical: false, collect: true,
		})
	if found != 0 || len(cycles) != 0 {
		t.Fatalf("repeated-vertex walk was reported as a cycle: found=%d cycles=%v",
			found, cycles)
	}
}

func TestBoundedCycleSearchSkipsInvalidRemoteIDs(t *testing.T) {
	// 0 <-> 1 is a valid two-cycle. The out-of-range ID must be ignored before
	// canonical-rank lookup and must not affect the valid result.
	adjacency := []serialize.SliceUint32{{1, 99}, {0}}
	ranks := []uint32{0, 1}
	state := newCycleSearchState(2)
	cycles, found := boundedCycleSearch(0, adjacency, ranks, &state,
		cycleSearchOptions{
			minLen: 2, maxLen: 2, canonical: true, collect: true,
		})
	if found != 1 || len(cycles) != 1 || len(cycles[0]) != 1 || cycles[0][0] != 1 {
		t.Fatalf("invalid remote ID changed valid cycle result: found=%d cycles=%v",
			found, cycles)
	}
}
