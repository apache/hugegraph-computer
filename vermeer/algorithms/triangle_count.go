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
	"vermeer/apps/options"
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

type TriangleCountWorker struct {
	compute.WorkerComputerBase
	triangleCount     []serialize.SInt64
	degrees           []serialize.SUint32
	orientedNeighbors []serialize.SliceUint32
	localNeighbors    []serialize.SliceUint32
}

func (tcw *TriangleCountWorker) VertexValue(vertexID uint32) serialize.MarshalAble {
	if tcw.WContext.Output {
		return &tcw.triangleCount[vertexID]
	}
	switch tcw.WContext.Step {
	case 1:
		return &tcw.degrees[vertexID]
	case 2:
		return &tcw.orientedNeighbors[vertexID]
	default:
		return &noTriangleScatterValue
	}
}

func (tcw *TriangleCountWorker) Init() error {
	if err := validateTriangleMaxStep(options.GetInt(tcw.WContext.Params,
		"compute.max_step")); err != nil {
		return err
	}
	totalVertices := tcw.WContext.GraphData.Vertex.TotalVertexCount()
	localVertices := tcw.WContext.GraphData.VertexCount
	tcw.triangleCount = make([]serialize.SInt64, totalVertices)
	tcw.degrees = make([]serialize.SUint32, totalVertices)
	tcw.orientedNeighbors = make([]serialize.SliceUint32, totalVertices)
	tcw.localNeighbors = make([]serialize.SliceUint32, localVertices)
	return nil
}

func (tcw *TriangleCountWorker) Compute(vertexID uint32, _ int) {
	localID := vertexID - tcw.WContext.GraphData.VertIDStart
	switch tcw.WContext.Step {
	case 1:
		neighbors := sortedUndirectedNeighbors(vertexID,
			uint32(len(tcw.degrees)),
			tcw.WContext.GraphData.Edges.GetInEdges(localID),
			tcw.WContext.GraphData.Edges.GetOutEdges(localID))
		tcw.localNeighbors[localID] = neighbors
		tcw.degrees[vertexID] = serialize.SUint32(len(neighbors))
	case 2:
		tcw.orientedNeighbors[vertexID] = orientedNeighbors(vertexID,
			tcw.localNeighbors[localID], tcw.degrees)
	case 3:
		// The largest simple graph addressable by uint32 vertex IDs has fewer
		// than MaxInt64 triangles incident to one vertex, so this cast is safe.
		tcw.triangleCount[vertexID] = serialize.SInt64(
			countVertexTriangles(tcw.localNeighbors[localID],
				tcw.orientedNeighbors))
	}
}

func (tcw *TriangleCountWorker) OutputValueType() string {
	return common.HgValueTypeLong
}

type TriangleCountMaster struct {
	compute.MasterComputerBase
}

func (tcm *TriangleCountMaster) Compute() bool {
	return tcm.MContext.Step != triangleComputeSteps
}
