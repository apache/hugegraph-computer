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
	"vermeer/apps/common"
	"vermeer/apps/compute"
	"vermeer/apps/serialize"
)

func init() {
	Algorithms = append(Algorithms, new(TriangleCountMaker))
}

type TriangleCountMaker struct {
	compute.AlgorithmMakerBase
}

func (tc *TriangleCountMaker) CreateWorkerComputer() compute.WorkerComputer {
	return &TriangleCountWorker{}
}

func (tc *TriangleCountMaker) CreateMasterComputer() compute.MasterComputer {
	return &TriangleCountMaster{}
}

func (tc *TriangleCountMaker) Name() string {
	return "triangle_count"
}

func (tc *TriangleCountMaker) DataNeeded() []string {
	return []string{compute.UseOutEdge}
}

// buildTriangleNeighborState treats incoming and outgoing edges as one
// undirected neighborhood, removes self-loops and duplicate parallel edges,
// and returns only the lower-ID half for cross-worker exchange. For every
// triangle incident to vertex v, exactly one of the other two vertices has the
// larger ID, so intersecting N(v) with each neighbor's lower-ID half counts the
// triangle once for v.
func buildTriangleNeighborState(vertexID uint32, inEdges,
	outEdges serialize.SliceUint32) (map[serialize.SUint32]struct{}, serialize.SliceUint32) {
	neighbors := make(map[serialize.SUint32]struct{}, len(inEdges)+len(outEdges))
	lower := make(serialize.SliceUint32, 0, (len(inEdges)+len(outEdges))/2)
	add := func(neighbor serialize.SUint32) {
		if uint32(neighbor) == vertexID {
			return
		}
		if _, exists := neighbors[neighbor]; exists {
			return
		}
		neighbors[neighbor] = struct{}{}
		if uint32(neighbor) < vertexID {
			lower = append(lower, neighbor)
		}
	}
	for _, neighbor := range inEdges {
		add(neighbor)
	}
	for _, neighbor := range outEdges {
		add(neighbor)
	}
	return neighbors, lower
}

func countTriangleMembership(neighbors map[serialize.SUint32]struct{},
	lowerNeighbors serialize.SliceUint32) serialize.SUint32 {
	var count serialize.SUint32
	for _, candidate := range lowerNeighbors {
		if _, exists := neighbors[candidate]; exists {
			count++
		}
	}
	return count
}

type TriangleCountWorker struct {
	compute.WorkerComputerBase
	triangleCount  []serialize.SUint32
	lowerNeighbors []serialize.SliceUint32
	localNeighbors []map[serialize.SUint32]struct{}
}

func (tcw *TriangleCountWorker) VertexValue(vertexID uint32) serialize.MarshalAble {
	if tcw.WContext.Output {
		return &tcw.triangleCount[vertexID]
	}
	if tcw.WContext.Step == 1 {
		return &tcw.lowerNeighbors[vertexID]
	}
	return &tcw.triangleCount[vertexID]
}

func (tcw *TriangleCountWorker) Init() error {
	totalVertices := tcw.WContext.GraphData.Vertex.TotalVertexCount()
	localVertices := tcw.WContext.GraphData.VertexCount
	tcw.triangleCount = make([]serialize.SUint32, totalVertices)
	// Slice headers are substantially cheaper than one Go map per global
	// vertex. Only local vertices retain hash sets for membership tests.
	tcw.lowerNeighbors = make([]serialize.SliceUint32, totalVertices)
	tcw.localNeighbors = make([]map[serialize.SUint32]struct{}, localVertices)
	return nil
}

func (tcw *TriangleCountWorker) Compute(vertexID uint32, pID int) {
	_ = pID
	localID := vertexID - tcw.WContext.GraphData.VertIDStart
	switch tcw.WContext.Step {
	case 1:
		neighbors, lower := buildTriangleNeighborState(vertexID,
			tcw.WContext.GraphData.Edges.GetInEdges(localID),
			tcw.WContext.GraphData.Edges.GetOutEdges(localID))
		tcw.localNeighbors[localID] = neighbors
		tcw.lowerNeighbors[vertexID] = lower
	case 2:
		neighbors := tcw.localNeighbors[localID]
		var count serialize.SUint32
		for neighborID := range neighbors {
			if int(neighborID) >= len(tcw.lowerNeighbors) {
				continue
			}
			count += countTriangleMembership(neighbors,
				tcw.lowerNeighbors[neighborID])
		}
		tcw.triangleCount[vertexID] = count
	}
}

func (tcw *TriangleCountWorker) AfterStep() {
	if tcw.WContext.Step != 2 {
		return
	}
	// Output needs only the final count. Release adjacency workspace before the
	// writer starts so a large graph is not held twice during result export.
	tcw.lowerNeighbors = nil
	tcw.localNeighbors = nil
}

func (tcw *TriangleCountWorker) OutputValueType() string {
	return common.HgValueTypeInt
}

type TriangleCountMaster struct {
	compute.MasterComputerBase
}

func (tcm *TriangleCountMaster) Compute() bool {
	return tcm.MContext.Step != 2
}
