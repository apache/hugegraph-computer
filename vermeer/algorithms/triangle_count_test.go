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

func TestBuildTriangleNeighborStateDeduplicatesAndDropsSelfLoops(t *testing.T) {
	neighbors, lower := buildTriangleNeighborState(3,
		serialize.SliceUint32{1, 2, 3, 2},
		serialize.SliceUint32{1, 4, 3, 4})
	if len(neighbors) != 3 {
		t.Fatalf("expected neighbors {1,2,4}, got %v", neighbors)
	}
	for _, expected := range []serialize.SUint32{1, 2, 4} {
		if _, exists := neighbors[expected]; !exists {
			t.Fatalf("missing neighbor %d in %v", expected, neighbors)
		}
	}
	if len(lower) != 2 || lower[0] != 1 || lower[1] != 2 {
		t.Fatalf("expected lower neighbors [1 2], got %v", lower)
	}
}

func TestTriangleCountReturnsOneForEveryTriangleVertex(t *testing.T) {
	// Every undirected edge is intentionally present in both directions. The
	// compact state must deduplicate it before counting.
	inEdges := []serialize.SliceUint32{
		{1, 2},
		{0, 2},
		{0, 1},
	}
	outEdges := []serialize.SliceUint32{
		{1, 2},
		{0, 2},
		{0, 1},
	}
	counts := triangleCountsForTest(inEdges, outEdges)
	for vertexID, count := range counts {
		if count != 1 {
			t.Fatalf("vertex %d expected one incident triangle, got %d", vertexID, count)
		}
	}
}

func TestTriangleCountDoesNotCountOpenWedge(t *testing.T) {
	inEdges := []serialize.SliceUint32{
		{1},
		{0, 2},
		{1},
	}
	outEdges := []serialize.SliceUint32{
		{1},
		{0, 2},
		{1},
	}
	counts := triangleCountsForTest(inEdges, outEdges)
	for vertexID, count := range counts {
		if count != 0 {
			t.Fatalf("open wedge vertex %d reported %d triangles", vertexID, count)
		}
	}
}

func TestTriangleCountHandlesMixedEdgeDirections(t *testing.T) {
	// The same undirected triangle is split across incoming and outgoing
	// storage, matching graphs that do not materialize reciprocal edges.
	inEdges := []serialize.SliceUint32{
		{2},
		{0},
		{1},
	}
	outEdges := []serialize.SliceUint32{
		{1},
		{2},
		{0},
	}
	counts := triangleCountsForTest(inEdges, outEdges)
	for vertexID, count := range counts {
		if count != 1 {
			t.Fatalf("mixed-direction vertex %d expected one triangle, got %d", vertexID, count)
		}
	}
}

func TestCountTriangleMembershipScansCompactLowerSlice(t *testing.T) {
	neighbors := map[serialize.SUint32]struct{}{
		1: {},
		3: {},
		5: {},
		7: {},
	}
	lower := serialize.SliceUint32{0, 1, 2, 3, 4, 5, 6}
	if got := countTriangleMembership(neighbors, lower); got != 3 {
		t.Fatalf("expected three shared neighbors, got %d", got)
	}
}

func TestTriangleCountIgnoresInvalidRemoteNeighborList(t *testing.T) {
	neighbors := []map[serialize.SUint32]struct{}{
		{1: {}, 99: {}},
		{0: {}},
	}
	lower := []serialize.SliceUint32{
		{},
		{0},
	}
	var count serialize.SUint32
	for neighborID := range neighbors[0] {
		if int(neighborID) >= len(lower) {
			continue
		}
		count += countTriangleMembership(neighbors[0], lower[neighborID])
	}
	if count != 0 {
		t.Fatalf("invalid remote ID changed triangle count: %d", count)
	}
}

func triangleCountsForTest(inEdges, outEdges []serialize.SliceUint32) []serialize.SUint32 {
	total := len(inEdges)
	neighbors := make([]map[serialize.SUint32]struct{}, total)
	lower := make([]serialize.SliceUint32, total)
	for vertexID := 0; vertexID < total; vertexID++ {
		neighbors[vertexID], lower[vertexID] = buildTriangleNeighborState(
			uint32(vertexID), inEdges[vertexID], outEdges[vertexID])
	}

	counts := make([]serialize.SUint32, total)
	for vertexID := 0; vertexID < total; vertexID++ {
		for neighborID := range neighbors[vertexID] {
			if int(neighborID) >= total {
				continue
			}
			counts[vertexID] += countTriangleMembership(neighbors[vertexID],
				lower[neighborID])
		}
	}
	return counts
}
