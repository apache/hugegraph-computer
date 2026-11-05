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
	"vermeer/apps/common"
	"vermeer/apps/compute"
	"vermeer/apps/options"
	"vermeer/apps/serialize"

	"github.com/sirupsen/logrus"
)

const cycleCountValue = "cycle_num"

func init() {
	Algorithms = append(Algorithms, new(CycleDetectionMaker))
}

type CycleDetectionMaker struct {
	compute.AlgorithmMakerBase
}

func (cd *CycleDetectionMaker) CreateWorkerComputer() compute.WorkerComputer {
	return &CycleDetectionWorker{}
}

func (cd *CycleDetectionMaker) CreateMasterComputer() compute.MasterComputer {
	return &CycleDetectionMaster{}
}

func (cd *CycleDetectionMaker) Name() string {
	return "cycle_detection"
}

func (cd *CycleDetectionMaker) DataNeeded() []string {
	return []string{}
}

type cyclesMode string

const (
	All     cyclesMode = "all"
	Limit   cyclesMode = "limit"
	Boolean cyclesMode = "boolean"
)

type CycleDetectionWorker struct {
	compute.WorkerComputerBase
	parameters      cycleParameters
	hasCycle        []serialize.SUint8
	inEdges         []serialize.SliceUint32
	cycleList       []serialize.TwoDimSliceString
	cycleRanks      []uint32
	searchStates    []cycleSearchState
	useVertexFilter bool
	useEdgeFilter   bool
	vertexFilter    *compute.VertexFilter
	edgeFilter      *compute.EdgeFilter
}

func (cdw *CycleDetectionWorker) Init() error {
	totalVertices := cdw.WContext.GraphData.Vertex.TotalVertexCount()
	parameters, err := parseCycleParameters(cdw.WContext.Params, totalVertices)
	if err != nil {
		return err
	}
	cdw.parameters = parameters
	cdw.inEdges = make([]serialize.SliceUint32, totalVertices)

	if parameters.mode != Boolean {
		cdw.cycleRanks = buildCycleRanksByID(totalVertices,
			func(vertexID uint32) string {
				return cdw.WContext.GraphData.Vertex.GetVertex(vertexID).ID
			})
	}
	parallel := cdw.WContext.Parallel
	if parallel <= 0 {
		parallel = 1
	}
	cdw.searchStates = make([]cycleSearchState, parallel)
	for partitionID := range cdw.searchStates {
		cdw.searchStates[partitionID] = newCycleSearchState(parameters.maxLen)
	}

	switch parameters.mode {
	case All, Limit:
		cdw.cycleList = make([]serialize.TwoDimSliceString,
			cdw.WContext.GraphData.VertexCount)
	case Boolean:
		cdw.hasCycle = make([]serialize.SUint8,
			cdw.WContext.GraphData.VertexCount)
	}
	cdw.WContext.CreateValue(cycleCountValue, compute.ValueTypeInt64,
		compute.CValueActionAggregate)
	cdw.WContext.SetValue(cycleCountValue, serialize.SInt64(0))

	vertexExprStr := options.GetString(cdw.WContext.Params, "filter.vertex_expr")
	filterVertexProps := options.GetSliceString(cdw.WContext.Params,
		"filter.vertex_properties")
	edgeExprStr := options.GetString(cdw.WContext.Params, "filter.edge_expr")
	filterEdgeProps := options.GetSliceString(cdw.WContext.Params,
		"filter.edge_properties")
	if vertexExprStr != "" && len(filterVertexProps) > 0 {
		cdw.useVertexFilter = true
		cdw.vertexFilter = &compute.VertexFilter{}
		if err := cdw.vertexFilter.Init(vertexExprStr, filterVertexProps,
			cdw.WContext.GraphData.VertexProperty); err != nil {
			return fmt.Errorf("vertex filter init error: %w", err)
		}
	}
	if edgeExprStr != "" && len(filterEdgeProps) > 0 {
		cdw.useEdgeFilter = true
		cdw.edgeFilter = &compute.EdgeFilter{}
		if err := cdw.edgeFilter.Init(edgeExprStr, filterEdgeProps,
			cdw.WContext.GraphData.InEdgesProperty); err != nil {
			return fmt.Errorf("edge filter init error: %w", err)
		}
	}
	return nil
}

func (cdw *CycleDetectionWorker) VertexValue(vertexID uint32) serialize.MarshalAble {
	if cdw.WContext.Output {
		localID := vertexID - cdw.WContext.GraphData.VertIDStart
		switch cdw.parameters.mode {
		case All, Limit:
			return &cdw.cycleList[localID]
		case Boolean:
			return &cdw.hasCycle[localID]
		}
	}
	if cdw.WContext.Step == 1 {
		return &cdw.inEdges[vertexID]
	}
	return &noCycleScatterValue
}

func (cdw *CycleDetectionWorker) Compute(vertexID uint32, partitionID int) {
	if cdw.useVertexFilter && !cdw.vertexFilter.Filter(vertexID) {
		return
	}
	localID := vertexID - cdw.WContext.GraphData.VertIDStart
	if cdw.WContext.Step == 1 {
		cdw.buildFilteredInEdges(vertexID, localID)
		return
	}
	if cdw.WContext.Step != 2 {
		return
	}

	state := &cdw.searchStates[partitionID]
	options := cycleSearchOptions{
		minLen:    cdw.parameters.minLen,
		maxLen:    cdw.parameters.maxLen,
		canonical: cdw.parameters.mode != Boolean,
		collect:   cdw.parameters.mode != Boolean,
	}
	switch cdw.parameters.mode {
	case All:
		cycles, _ := boundedCycleSearch(vertexID, cdw.inEdges,
			cdw.cycleRanks, state, options)
		cdw.cycleList[localID] = cdw.externalCycleIDs(cycles)
	case Limit:
		options.maxResults = cdw.parameters.limit
		cycles, _ := boundedCycleSearch(vertexID, cdw.inEdges,
			cdw.cycleRanks, state, options)
		cdw.cycleList[localID] = cdw.externalCycleIDs(cycles)
	case Boolean:
		options.maxResults = 1
		_, found := boundedCycleSearch(vertexID, cdw.inEdges,
			cdw.cycleRanks, state, options)
		if found > 0 {
			cdw.hasCycle[localID] = 1
		}
	}
}

func (cdw *CycleDetectionWorker) buildFilteredInEdges(vertexID, localID uint32) {
	rawEdges := cdw.WContext.GraphData.Edges.GetInEdges(localID)
	neighbors := make(serialize.SliceUint32, 0, len(rawEdges))
	for edgeIndex, edge := range rawEdges {
		neighborID := uint32(edge)
		if neighborID == vertexID {
			continue
		}
		if cdw.useVertexFilter && !cdw.vertexFilter.Filter(neighborID) {
			continue
		}
		if cdw.useEdgeFilter && !cdw.edgeFilter.Filter(localID,
			uint32(edgeIndex)) {
			continue
		}
		neighbors = append(neighbors, edge)
	}
	cdw.inEdges[vertexID] = sortedUniqueCycleNeighbors(neighbors)
}

func (cdw *CycleDetectionWorker) externalCycleIDs(cycles [][]uint32) serialize.TwoDimSliceString {
	result := make(serialize.TwoDimSliceString, 0, len(cycles))
	for _, cycle := range cycles {
		ids := make(serialize.SliceString, len(cycle))
		for index, vertexID := range cycle {
			ids[index] = serialize.SString(
				cdw.WContext.GraphData.Vertex.GetVertex(vertexID).ID)
		}
		result = append(result, ids)
	}
	return result
}

func (cdw *CycleDetectionWorker) AfterStep() {
	if cdw.WContext.Step != 2 {
		return
	}
	var cycleCount int64
	switch cdw.parameters.mode {
	case All, Limit:
		for _, cycles := range cdw.cycleList {
			cycleCount += int64(len(cycles))
		}
	case Boolean:
		for _, hasCycle := range cdw.hasCycle {
			if hasCycle == 1 {
				cycleCount++
			}
		}
	}
	cdw.WContext.SetValue(cycleCountValue, serialize.SInt64(cycleCount))
}

func (cdw *CycleDetectionWorker) OutputValueType() string {
	if cdw.parameters.mode == Boolean {
		return common.HgValueTypeInt
	}
	return common.HgValueTypeString
}

type CycleDetectionMaster struct {
	compute.MasterComputerBase
}

func (cdm *CycleDetectionMaster) Compute() bool {
	if cdm.MContext.Step != 2 {
		return true
	}
	cycleCount := cdm.MContext.GetValue(cycleCountValue).(serialize.SInt64)
	logrus.Infof("cycles num:%v", cycleCount)
	return false
}
