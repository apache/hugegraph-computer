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
	"fmt"
	"sort"
	"vermeer/apps/options"
	"vermeer/apps/serialize"
)

type cycleParameters struct {
	mode   cyclesMode
	minLen int
	maxLen int
	limit  int
}

func parseCycleParameters(params map[string]string, totalVertices uint32) (cycleParameters, error) {
	result := cycleParameters{
		mode:   cyclesMode(options.GetString(params, "cycle.mode")),
		minLen: options.GetInt(params, "cycle.min_length"),
		maxLen: options.GetInt(params, "cycle.max_length"),
		limit:  options.GetInt(params, "cycle.max_cycles"),
	}
	if options.GetInt(params, "compute.max_step") < 2 {
		return result, fmt.Errorf("cycle_detection requires compute.max_step >= 2")
	}
	if result.minLen < 0 {
		return result, fmt.Errorf("cycle.min_length must be >= 0, got %d", result.minLen)
	}
	if result.maxLen < 2 {
		return result, fmt.Errorf("cycle.max_length must be >= 2, got %d", result.maxLen)
	}
	// Self-loops are excluded, so the shortest supported simple directed cycle
	// contains two distinct vertices. Keep the historical default min_length=0
	// as an alias for the natural lower bound.
	if result.minLen < 2 {
		result.minLen = 2
	}
	if result.minLen > result.maxLen {
		return result, fmt.Errorf("cycle.min_length (%d) must not exceed cycle.max_length (%d)",
			result.minLen, result.maxLen)
	}
	if totalVertices >= 2 && result.maxLen > int(totalVertices) {
		result.maxLen = int(totalVertices)
	}
	switch result.mode {
	case All, Boolean:
	case Limit:
		if result.limit <= 0 {
			return result, fmt.Errorf("cycle.max_cycles must be > 0 in limit mode, got %d",
				result.limit)
		}
	default:
		return result, fmt.Errorf("cycle detection mode must be 'all', 'limit', or 'boolean', got %q",
			result.mode)
	}
	return result, nil
}

func buildCycleRanks(vertexIDs []string) []uint32 {
	return buildCycleRanksByID(uint32(len(vertexIDs)), func(vertexID uint32) string {
		return vertexIDs[vertexID]
	})
}

// buildCycleRanksByID reads external IDs from their existing graph storage.
// It intentionally allocates only one temporary uint32 permutation and the
// retained uint32 rank vector, rather than copying one string header per vertex.
func buildCycleRanksByID(totalVertices uint32,
	vertexIDAt func(uint32) string) []uint32 {
	order := make([]uint32, totalVertices)
	for vertexID := range order {
		order[vertexID] = uint32(vertexID)
	}
	sort.Slice(order, func(left, right int) bool {
		leftID := order[left]
		rightID := order[right]
		leftExternalID := vertexIDAt(leftID)
		rightExternalID := vertexIDAt(rightID)
		if leftExternalID != rightExternalID {
			return leftExternalID < rightExternalID
		}
		return leftID < rightID
	})
	ranks := make([]uint32, totalVertices)
	for rank, vertexID := range order {
		ranks[vertexID] = uint32(rank)
	}
	return ranks
}

func sortedUniqueCycleNeighbors(neighbors serialize.SliceUint32) serialize.SliceUint32 {
	if len(neighbors) < 2 {
		return neighbors
	}
	sort.Slice(neighbors, func(left, right int) bool {
		return neighbors[left] < neighbors[right]
	})
	write := 1
	for read := 1; read < len(neighbors); read++ {
		if neighbors[read] == neighbors[write-1] {
			continue
		}
		neighbors[write] = neighbors[read]
		write++
	}
	return neighbors[:write]
}

type cycleFrame struct {
	vertexID uint32
	nextEdge int
}

type cycleSearchState struct {
	visited map[uint32]struct{}
	path    []uint32
	frames  []cycleFrame
}

func newCycleSearchState(maxLen int) cycleSearchState {
	capacity := maxLen
	if capacity > 64 {
		capacity = 64
	}
	return cycleSearchState{
		visited: make(map[uint32]struct{}, capacity),
		path:    make([]uint32, 0, capacity),
		frames:  make([]cycleFrame, 0, capacity),
	}
}

func (state *cycleSearchState) reset(rootID uint32) {
	clear(state.visited)
	state.path = append(state.path[:0], rootID)
	state.frames = append(state.frames[:0], cycleFrame{vertexID: rootID})
	state.visited[rootID] = struct{}{}
}

type cycleSearchOptions struct {
	minLen     int
	maxLen     int
	maxResults int
	canonical  bool
	collect    bool
}

func boundedCycleSearch(rootID uint32, adjacency []serialize.SliceUint32,
	ranks []uint32, state *cycleSearchState,
	options cycleSearchOptions) ([][]uint32, int) {
	if int(rootID) >= len(adjacency) || options.maxLen < 2 {
		return nil, 0
	}
	state.reset(rootID)
	var rootRank uint32
	if options.canonical {
		if int(rootID) >= len(ranks) {
			return nil, 0
		}
		rootRank = ranks[rootID]
	}
	var cycles [][]uint32
	found := 0

	for len(state.frames) > 0 {
		frameIndex := len(state.frames) - 1
		frame := &state.frames[frameIndex]
		neighbors := adjacency[frame.vertexID]
		if frame.nextEdge >= len(neighbors) {
			vertexID := frame.vertexID
			state.frames = state.frames[:frameIndex]
			state.path = state.path[:len(state.path)-1]
			delete(state.visited, vertexID)
			continue
		}

		neighborID := uint32(neighbors[frame.nextEdge])
		frame.nextEdge++
		if neighborID == rootID {
			cycleLen := len(state.path)
			if cycleLen >= options.minLen && cycleLen <= options.maxLen {
				found++
				if options.collect {
					cycle := make([]uint32, len(state.path)-1)
					copy(cycle, state.path[1:])
					cycles = append(cycles, cycle)
				}
				if options.maxResults > 0 && found >= options.maxResults {
					return cycles, found
				}
			}
			continue
		}
		if neighborID >= uint32(len(adjacency)) || len(state.path) >= options.maxLen {
			continue
		}
		if options.canonical && ranks[neighborID] <= rootRank {
			continue
		}
		if _, seen := state.visited[neighborID]; seen {
			continue
		}

		state.visited[neighborID] = struct{}{}
		state.path = append(state.path, neighborID)
		state.frames = append(state.frames, cycleFrame{vertexID: neighborID})
	}
	return cycles, found
}

type emptyCycleScatterValue struct{}

func (*emptyCycleScatterValue) Marshal([]byte) (int, error) {
	return 0, nil
}

func (*emptyCycleScatterValue) Unmarshal([]byte) (int, error) {
	return 0, nil
}

func (*emptyCycleScatterValue) ToString() string {
	return ""
}

func (*emptyCycleScatterValue) PredictSize() int {
	return 0
}

var noCycleScatterValue emptyCycleScatterValue
