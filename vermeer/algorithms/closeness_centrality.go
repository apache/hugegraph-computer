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
	"math"
	"math/rand"
	"sync/atomic"
	"vermeer/apps/common"
	"vermeer/apps/compute"
	"vermeer/apps/options"
	"vermeer/apps/serialize"

	"github.com/sirupsen/logrus"
)

const closenessUnfinishedValue = "closeness_unfinished"

func init() {
	Algorithms = append(Algorithms, new(ClosenessCentralityMaker))
}

// normalized closeness centrality
type ClosenessCentralityMaker struct {
	compute.AlgorithmMakerBase
}

func (cc *ClosenessCentralityMaker) CreateWorkerComputer() compute.WorkerComputer {
	return &ClosenessCentralityWorker{}
}

func (cc *ClosenessCentralityMaker) CreateMasterComputer() compute.MasterComputer {
	return &ClosenessCentralityMaster{}
}

func (cc *ClosenessCentralityMaker) Name() string {
	return "closeness_centrality"
}

func (cc *ClosenessCentralityMaker) DataNeeded() []string {
	return []string{}
}

func validateProbability(name string, value float64) error {
	if math.IsNaN(value) || math.IsInf(value, 0) || value <= 0 || value > 1 {
		return fmt.Errorf("%s must be finite and in (0, 1], got %v", name, value)
	}
	return nil
}

func parseBooleanFlag(name string, value int) (bool, error) {
	switch value {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, fmt.Errorf("%s must be 0 or 1, got %d", name, value)
	}
}

func newClosenessDistanceMap(vertexID uint32, capacity int) map[uint32]int32 {
	distances := make(map[uint32]int32, capacity+1)
	distances[vertexID] = 0
	return distances
}

func closenessScore(distances map[uint32]int32, totalVertices uint32,
	wfImproved bool) serialize.SFloat32 {
	if totalVertices <= 1 || len(distances) <= 1 {
		return 0
	}

	var distanceSum int64
	for _, distance := range distances {
		distanceSum += int64(distance)
	}
	if distanceSum <= 0 {
		return 0
	}

	reachable := len(distances) - 1
	score := float64(reachable) / float64(distanceSum)
	if wfImproved {
		score *= float64(reachable) / float64(totalVertices-1)
	}
	return serialize.SFloat32(score)
}

type ClosenessCentralityWorker struct {
	compute.WorkerComputerBase
	wfImproved      bool
	sampleRate      float64
	closenessValues []serialize.SFloat32
	tempEdges       [][]serialize.SUint32
	dfsOutsideEdge  [][]serialize.SUint32
	neighborEdges   []serialize.SliceUint32
	pMap            []map[uint32]int32
	finished        []bool
	unfinished      int64
}

func (ccw *ClosenessCentralityWorker) VertexValue(i uint32) serialize.MarshalAble {
	if ccw.WContext.Output {
		return &ccw.closenessValues[i]
	}
	if ccw.WContext.Step == 1 {
		return &ccw.neighborEdges[i]
	}
	return &ccw.closenessValues[i]
}

func (ccw *ClosenessCentralityWorker) ValidateOutput() error {
	cValue, ok := ccw.WContext.CValues[closenessUnfinishedValue]
	if !ok || cValue.Value == nil {
		return fmt.Errorf("closeness_centrality output is missing global convergence state")
	}
	unfinished, ok := cValue.Value.(serialize.SInt64)
	if !ok {
		return fmt.Errorf("closeness_centrality output has invalid convergence state %T",
			cValue.Value)
	}
	if unfinished != 0 {
		return fmt.Errorf("closeness_centrality did not converge for %d vertices; increase compute.max_step",
			unfinished)
	}
	return nil
}

func (ccw *ClosenessCentralityWorker) Init() error {
	sampleRate := options.GetFloat(ccw.WContext.Params,
		"closeness_centrality.sample_rate")
	if err := validateProbability("closeness_centrality.sample_rate", sampleRate); err != nil {
		return err
	}
	wfImproved, err := parseBooleanFlag("closeness_centrality.wf_improved",
		options.GetInt(ccw.WContext.Params, "closeness_centrality.wf_improved"))
	if err != nil {
		return err
	}
	ccw.sampleRate = sampleRate
	ccw.wfImproved = wfImproved

	totalVertices := ccw.WContext.GraphData.Vertex.TotalVertexCount()
	localVertices := ccw.WContext.GraphData.VertexCount
	ccw.closenessValues = make([]serialize.SFloat32, totalVertices)
	ccw.neighborEdges = make([]serialize.SliceUint32, totalVertices)
	ccw.dfsOutsideEdge = make([][]serialize.SUint32, localVertices)
	ccw.pMap = make([]map[uint32]int32, localVertices)
	for i := range ccw.pMap {
		localID := uint32(i)
		vertexID := ccw.WContext.GraphData.VertIDStart + localID
		ccw.pMap[i] = newClosenessDistanceMap(vertexID,
			len(ccw.WContext.GraphData.Edges.GetInEdges(localID)))
	}
	ccw.tempEdges = make([][]serialize.SUint32, ccw.WContext.Parallel)
	for i := range ccw.tempEdges {
		ccw.tempEdges[i] = make([]serialize.SUint32, 0, 100)
	}
	ccw.finished = make([]bool, localVertices)
	atomic.StoreInt64(&ccw.unfinished, int64(localVertices))

	ccw.WContext.CreateValue(closenessUnfinishedValue, compute.ValueTypeInt64,
		compute.CValueActionAggregate)
	ccw.WContext.SetValue(closenessUnfinishedValue, serialize.SInt64(localVertices))
	return nil
}

func (ccw *ClosenessCentralityWorker) Compute(vertexID uint32, pID int) {
	vertID := vertexID - ccw.WContext.GraphData.VertIDStart
	if ccw.finished[vertID] {
		return
	}

	switch ccw.WContext.Step {
	case 1:
		inEdges := ccw.WContext.GraphData.Edges.GetInEdges(vertID)
		ccw.neighborEdges[vertexID] = make([]serialize.SUint32, 0, len(inEdges))
		for _, edge := range inEdges {
			if !ccw.sample() {
				continue
			}
			if _, seen := ccw.pMap[vertID][uint32(edge)]; seen {
				continue
			}
			ccw.neighborEdges[vertexID] = append(ccw.neighborEdges[vertexID], edge)
			ccw.pMap[vertID][uint32(edge)] = 1
		}
		ccw.dfsOutsideEdge[vertID] = ccw.neighborEdges[vertexID]
	default:
		for _, value := range ccw.dfsOutsideEdge[vertID] {
			for _, neighborID := range ccw.neighborEdges[value] {
				if _, seen := ccw.pMap[vertID][uint32(neighborID)]; seen {
					continue
				}
				ccw.pMap[vertID][uint32(neighborID)] =
					ccw.pMap[vertID][uint32(value)] + 1
				ccw.tempEdges[pID] = append(ccw.tempEdges[pID], neighborID)
			}
		}
		ccw.dfsOutsideEdge[vertID] = append([]serialize.SUint32(nil),
			ccw.tempEdges[pID]...)
		ccw.tempEdges[pID] = ccw.tempEdges[pID][:0]
	}
}

func (ccw *ClosenessCentralityWorker) AfterStep() {
	for localID := range ccw.finished {
		if ccw.finished[localID] || ccw.frontierCanExpand(uint32(localID)) {
			continue
		}
		vertexID := ccw.WContext.GraphData.VertIDStart + uint32(localID)
		ccw.finishVertex(uint32(localID), vertexID)
	}
	ccw.WContext.SetValue(closenessUnfinishedValue,
		serialize.SInt64(atomic.LoadInt64(&ccw.unfinished)))
}

func (ccw *ClosenessCentralityWorker) frontierCanExpand(vertID uint32) bool {
	for _, frontierID := range ccw.dfsOutsideEdge[vertID] {
		for _, neighborID := range ccw.neighborEdges[frontierID] {
			if _, seen := ccw.pMap[vertID][uint32(neighborID)]; !seen {
				return true
			}
		}
	}
	return false
}

func (ccw *ClosenessCentralityWorker) finishVertex(vertID, vertexID uint32) {
	if ccw.finished[vertID] {
		return
	}
	ccw.closenessValues[vertexID] = closenessScore(ccw.pMap[vertID],
		ccw.WContext.GraphData.Vertex.TotalVertexCount(), ccw.wfImproved)
	ccw.finished[vertID] = true
	atomic.AddInt64(&ccw.unfinished, -1)
}

func (ccw *ClosenessCentralityWorker) sample() bool {
	return ccw.sampleRate == 1 || rand.Float64() < ccw.sampleRate
}

func (ccw *ClosenessCentralityWorker) OutputValueType() string {
	return common.HgValueTypeFloat
}

type ClosenessCentralityMaster struct {
	compute.MasterComputerBase
}

func (ccm *ClosenessCentralityMaster) Compute() bool {
	unfinished := ccm.MContext.GetValue(closenessUnfinishedValue).(serialize.SInt64)
	logrus.Infof("closeness unfinished vertices: %v", unfinished)
	return unfinished != 0
}
