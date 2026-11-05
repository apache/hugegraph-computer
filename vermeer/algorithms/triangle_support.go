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
	"vermeer/apps/serialize"
)

const triangleComputeSteps = 3

func validateTriangleMaxStep(maxStep int) error {
	if maxStep < triangleComputeSteps {
		return fmt.Errorf("triangle-based algorithms require compute.max_step >= %d, got %d",
			triangleComputeSteps, maxStep)
	}
	return nil
}

func sortedUndirectedNeighbors(vertexID, totalVertices uint32, inEdges,
	outEdges serialize.SliceUint32) serialize.SliceUint32 {
	neighbors := make(serialize.SliceUint32, 0, len(inEdges)+len(outEdges))
	for _, neighbor := range inEdges {
		neighborID := uint32(neighbor)
		if neighborID != vertexID && neighborID < totalVertices {
			neighbors = append(neighbors, neighbor)
		}
	}
	for _, neighbor := range outEdges {
		neighborID := uint32(neighbor)
		if neighborID != vertexID && neighborID < totalVertices {
			neighbors = append(neighbors, neighbor)
		}
	}
	if len(neighbors) < 2 {
		return neighbors
	}

	sort.Slice(neighbors, func(i, j int) bool {
		return neighbors[i] < neighbors[j]
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

func rankBefore(leftID, rightID uint32, degrees []serialize.SUint32) bool {
	leftDegree := degrees[leftID]
	rightDegree := degrees[rightID]
	if leftDegree != rightDegree {
		return leftDegree < rightDegree
	}
	return leftID < rightID
}

func orientedNeighbors(vertexID uint32, neighbors serialize.SliceUint32,
	degrees []serialize.SUint32) serialize.SliceUint32 {
	count := 0
	for _, rawNeighbor := range neighbors {
		neighborID := uint32(rawNeighbor)
		if neighborID < uint32(len(degrees)) &&
			rankBefore(vertexID, neighborID, degrees) {
			count++
		}
	}
	if count == 0 {
		return nil
	}

	oriented := make(serialize.SliceUint32, 0, count)
	for _, rawNeighbor := range neighbors {
		neighborID := uint32(rawNeighbor)
		if neighborID < uint32(len(degrees)) &&
			rankBefore(vertexID, neighborID, degrees) {
			oriented = append(oriented, rawNeighbor)
		}
	}
	return oriented
}

func sortedIntersectionCount(left, right serialize.SliceUint32) uint64 {
	if len(left) == 0 || len(right) == 0 {
		return 0
	}
	if len(left) > len(right) {
		left, right = right, left
	}

	// A binary-search intersection avoids scanning a high-degree adjacency list
	// once for every low-degree oriented neighbor. For similar-sized slices, the
	// linear merge has better constants and cache locality.
	if len(right) > 8*len(left) {
		var count uint64
		for _, value := range left {
			index := sort.Search(len(right), func(index int) bool {
				return right[index] >= value
			})
			if index < len(right) && right[index] == value {
				count++
			}
		}
		return count
	}

	var count uint64
	leftIndex := 0
	rightIndex := 0
	for leftIndex < len(left) && rightIndex < len(right) {
		switch {
		case left[leftIndex] < right[rightIndex]:
			leftIndex++
		case left[leftIndex] > right[rightIndex]:
			rightIndex++
		default:
			count++
			leftIndex++
			rightIndex++
		}
	}
	return count
}

func countVertexTriangles(neighbors serialize.SliceUint32,
	orientedByVertex []serialize.SliceUint32) uint64 {
	var count uint64
	for _, neighbor := range neighbors {
		neighborID := uint32(neighbor)
		if neighborID >= uint32(len(orientedByVertex)) {
			continue
		}
		// For each pair of neighbors in a triangle, exactly one endpoint is
		// earlier in the (degree, vertex-ID) order. Its oriented adjacency
		// therefore counts the pair once, independent of the root vertex rank.
		count += sortedIntersectionCount(neighbors,
			orientedByVertex[neighborID])
	}
	return count
}

type emptyTriangleScatterValue struct{}

func (*emptyTriangleScatterValue) Marshal([]byte) (int, error) {
	return 0, nil
}

func (*emptyTriangleScatterValue) Unmarshal(buffer []byte) (int, error) {
	if len(buffer) != 0 {
		return 0, fmt.Errorf("unexpected triangle scatter payload: %d bytes", len(buffer))
	}
	return 0, nil
}

func (*emptyTriangleScatterValue) ToString() string {
	return ""
}

func (*emptyTriangleScatterValue) PredictSize() int {
	return 0
}

var noTriangleScatterValue emptyTriangleScatterValue
