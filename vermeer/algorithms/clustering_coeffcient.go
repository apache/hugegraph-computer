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
	Algorithms = append(Algorithms, new(ClusteringCoefficientMaker))
}

type ClusteringCoefficientMaker struct {
	compute.AlgorithmMakerBase
}

func (cc *ClusteringCoefficientMaker) CreateWorkerComputer() compute.WorkerComputer {
	return &ClusteringCoefficientWorker{}
}

func (cc *ClusteringCoefficientMaker) CreateMasterComputer() compute.MasterComputer {
	return &ClusteringCoefficientMaster{}
}

func (cc *ClusteringCoefficientMaker) Name() string {
	return "clustering_coefficient"
}

func (cc *ClusteringCoefficientMaker) DataNeeded() []string {
	return []string{compute.UseOutEdge}
}

type ClusteringCoefficientWorker struct {
	compute.WorkerComputerBase
	ccValues          []serialize.SFloat32
	degrees           []serialize.SUint32
	orientedNeighbors []serialize.SliceUint32
	localNeighbors    []serialize.SliceUint32
}

func (ccw *ClusteringCoefficientWorker) VertexValue(vertexID uint32) serialize.MarshalAble {
	if ccw.WContext.Output {
		return &ccw.ccValues[vertexID]
	}
	switch ccw.WContext.Step {
	case 1:
		return &ccw.degrees[vertexID]
	case 2:
		return &ccw.orientedNeighbors[vertexID]
	default:
		return &noTriangleScatterValue
	}
}

func (ccw *ClusteringCoefficientWorker) Init() error {
	if err := validateTriangleMaxStep(options.GetInt(ccw.WContext.Params,
		"compute.max_step")); err != nil {
		return err
	}
	totalVertices := ccw.WContext.GraphData.Vertex.TotalVertexCount()
	localVertices := ccw.WContext.GraphData.VertexCount
	ccw.ccValues = make([]serialize.SFloat32, totalVertices)
	ccw.degrees = make([]serialize.SUint32, totalVertices)
	ccw.orientedNeighbors = make([]serialize.SliceUint32, totalVertices)
	ccw.localNeighbors = make([]serialize.SliceUint32, localVertices)
	return nil
}

func (ccw *ClusteringCoefficientWorker) Compute(vertexID uint32, _ int) {
	localID := vertexID - ccw.WContext.GraphData.VertIDStart
	switch ccw.WContext.Step {
	case 1:
		neighbors := sortedUndirectedNeighbors(vertexID,
			uint32(len(ccw.degrees)),
			ccw.WContext.GraphData.Edges.GetInEdges(localID),
			ccw.WContext.GraphData.Edges.GetOutEdges(localID))
		ccw.localNeighbors[localID] = neighbors
		ccw.degrees[vertexID] = serialize.SUint32(len(neighbors))
	case 2:
		ccw.orientedNeighbors[vertexID] = orientedNeighbors(vertexID,
			ccw.localNeighbors[localID], ccw.degrees)
	case 3:
		degree := len(ccw.localNeighbors[localID])
		if degree < 2 {
			ccw.ccValues[vertexID] = 0
			return
		}
		triangles := countVertexTriangles(ccw.localNeighbors[localID],
			ccw.orientedNeighbors)
		denominator := float64(degree) * float64(degree-1)
		ccw.ccValues[vertexID] = serialize.SFloat32(
			2 * float64(triangles) / denominator)
	}
}

func (ccw *ClusteringCoefficientWorker) OutputValueType() string {
	return common.HgValueTypeFloat
}

type ClusteringCoefficientMaster struct {
	compute.MasterComputerBase
}

func (ccm *ClusteringCoefficientMaster) Compute() bool {
	return ccm.MContext.Step != triangleComputeSteps
}
