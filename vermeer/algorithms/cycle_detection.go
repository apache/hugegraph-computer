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

const cycleCountValue = "cycle_num"

type cycleSearchConfig struct {
	minLength int
	maxLength int
	limit     int
}

func validateCycleSearchConfig(mode cyclesMode, minLength, maxLength, limit int) (cycleSearchConfig, error) {
	if maxLength < 2 {
		return cycleSearchConfig{}, fmt.Errorf("cycle.max_length must be at least 2, got %d", maxLength)
	}
	if minLength < 0 {
		return cycleSearchConfig{}, fmt.Errorf("cycle.min_length must be non-negative, got %d", minLength)
	}
	if minLength == 0 {
		minLength = 2
	}
	if minLength > maxLength {
		return cycleSearchConfig{}, fmt.Errorf("cycle.min_length %d exceeds cycle.max_length %d",
			minLength, maxLength)
	}

	switch mode {
	case All:
		return cycleSearchConfig{minLength: minLength, maxLength: maxLength}, nil
	case Limit:
		if limit <= 0 {
			return cycleSearchConfig{}, fmt.Errorf("cycle.max_cycles must be positive in limit mode, got %d", limit)
		}
		return cycleSearchConfig{minLength: minLength, maxLength: maxLength, limit: limit}, nil
	case Boolean:
		return cycleSearchConfig{minLength: minLength, maxLength: maxLength, limit: 1}, nil
	default:
		return cycleSearchConfig{}, fmt.Errorf("cycle detection mode must be 'all', 'limit', or 'boolean', got %q", mode)
	}
}

// searchBoundedCycles enumerates simple directed cycles that start and end at root.
// path always includes root, so len(path) is the real cycle length when an edge
// returns to root. A single visited set is mutated in place instead of rebuilding
// one map at every recursive level.
func searchBoundedCycles(root serialize.SUint32, adjacency []serialize.SliceUint32,
	config cycleSearchConfig, allow func(serialize.SUint32) bool,
	emit func([]serialize.SUint32)) int {
	if int(root) >= len(adjacency) {
		return 0
	}

	capacity := config.maxLength
	if capacity > 64 {
		capacity = 64
	}
	path := make([]serialize.SUint32, 1, capacity)
	path[0] = root
	visited := map[serialize.SUint32]struct{}{root: {}}
	count := 0

	var dfs func(serialize.SUint32)
	dfs = func(vertex serialize.SUint32) {
		if config.limit > 0 && count >= config.limit {
			return
		}
		for _, next := range adjacency[vertex] {
			if config.limit > 0 && count >= config.limit {
				return
			}
			if int(next) >= len(adjacency) {
				continue
			}
			if next == root {
				cycleLength := len(path)
				if cycleLength >= config.minLength && cycleLength <= config.maxLength {
					// The root is implied by the output vertex, preserving the existing
					// result shape while fixing length accounting.
					cycle := append([]serialize.SUint32(nil), path[1:]...)
					emit(cycle)
					count++
				}
				continue
			}
			if len(path) >= config.maxLength {
				continue
			}
			if allow != nil && !allow(next) {
				continue
			}
			if _, exists := visited[next]; exists {
				continue
			}

			visited[next] = struct{}{}
			path = append(path, next)
			dfs(next)
			path = path[:len(path)-1]
			delete(visited, next)
		}
	}

	dfs(root)
	return count
}

type CycleDetectionWorker struct {
	compute.WorkerComputerBase
	mode            cyclesMode
	searchConfig    cycleSearchConfig
	hasCycle        []serialize.SUint8
	inEdges         []serialize.SliceUint32
	cycleList       []serialize.TwoDimSliceString
	useVertexFilter bool
	useEdgeFilter   bool
	vertexFilter    *compute.VertexFilter
	edgeFilter      *compute.EdgeFilter
}

func (cdw *CycleDetectionWorker) Init() error {
	mode := cyclesMode(options.GetString(cdw.WContext.Params, "cycle.mode"))
	config, err := validateCycleSearchConfig(mode,
		options.GetInt(cdw.WContext.Params, "cycle.min_length"),
		options.GetInt(cdw.WContext.Params, "cycle.max_length"),
		options.GetInt(cdw.WContext.Params, "cycle.max_cycles"))
	if err != nil {
		return err
	}
	cdw.mode = mode
	cdw.searchConfig = config

	totalVertices := cdw.WContext.GraphData.Vertex.TotalVertexCount()
	localVertices := cdw.WContext.GraphData.VertexCount
	cdw.inEdges = make([]serialize.SliceUint32, totalVertices)
	switch cdw.mode {
	case All, Limit:
		cdw.cycleList = make([]serialize.TwoDimSliceString, localVertices)
	case Boolean:
		cdw.hasCycle = make([]serialize.SUint8, localVertices)
	}
	cdw.WContext.CreateValue(cycleCountValue, compute.ValueTypeInt64, compute.CValueActionAggregate)
	cdw.WContext.SetValue(cycleCountValue, serialize.SInt64(0))

	vertexExprStr := options.GetString(cdw.WContext.Params, "filter.vertex_expr")
	filterVertexProps := options.GetSliceString(cdw.WContext.Params, "filter.vertex_properties")
	edgeExprStr := options.GetString(cdw.WContext.Params, "filter.edge_expr")
	filterEdgeProps := options.GetSliceString(cdw.WContext.Params, "filter.edge_properties")
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
		switch cdw.mode {
		case All, Limit:
			return &cdw.cycleList[localID]
		case Boolean:
			return &cdw.hasCycle[localID]
		}
	}
	if cdw.WContext.Step == 1 {
		return &cdw.inEdges[vertexID]
	}
	nilValue := serialize.SInt32(0)
	return &nilValue
}

func (cdw *CycleDetectionWorker) Compute(vertexID uint32, pID int) {
	_ = pID
	if cdw.useVertexFilter && !cdw.vertexFilter.Filter(vertexID) {
		return
	}
	localID := vertexID - cdw.WContext.GraphData.VertIDStart
	if cdw.WContext.Step == 1 {
		cdw.collectIncomingEdges(vertexID, localID)
		return
	}

	switch cdw.mode {
	case All, Limit:
		rootExternalID := cdw.WContext.GraphData.Vertex.GetVertex(vertexID).ID
		allow := func(next serialize.SUint32) bool {
			return rootExternalID < cdw.WContext.GraphData.Vertex.GetVertex(uint32(next)).ID
		}
		searchBoundedCycles(serialize.SUint32(vertexID), cdw.inEdges, cdw.searchConfig,
			allow, func(cycle []serialize.SUint32) {
				output := make([]serialize.SString, 0, len(cycle))
				for _, node := range cycle {
					output = append(output,
						serialize.SString(cdw.WContext.GraphData.Vertex.GetVertex(uint32(node)).ID))
				}
				cdw.cycleList[localID] = append(cdw.cycleList[localID], output)
			})
	case Boolean:
		searchBoundedCycles(serialize.SUint32(vertexID), cdw.inEdges, cdw.searchConfig,
			nil, func([]serialize.SUint32) {
				cdw.hasCycle[localID] = 1
			})
	}
}

func (cdw *CycleDetectionWorker) collectIncomingEdges(vertexID, localID uint32) {
	edges := cdw.WContext.GraphData.Edges.GetInEdges(localID)
	cdw.inEdges[vertexID] = make(serialize.SliceUint32, 0, len(edges))
	seen := make(map[serialize.SUint32]struct{}, len(edges))
	for edgeIndex, edge := range edges {
		if uint32(edge) == vertexID {
			continue
		}
		if cdw.useVertexFilter && !cdw.vertexFilter.Filter(uint32(edge)) {
			continue
		}
		if cdw.useEdgeFilter && !cdw.edgeFilter.Filter(localID, uint32(edgeIndex)) {
			continue
		}
		if _, exists := seen[edge]; exists {
			continue
		}
		seen[edge] = struct{}{}
		cdw.inEdges[vertexID] = append(cdw.inEdges[vertexID], edge)
	}
}

func (cdw *CycleDetectionWorker) AfterStep() {
	if cdw.WContext.Step <= 1 {
		return
	}
	var cycleCount int64
	switch cdw.mode {
	case All, Limit:
		for _, list := range cdw.cycleList {
			cycleCount += int64(len(list))
		}
	case Boolean:
		for _, has := range cdw.hasCycle {
			if has == 1 {
				cycleCount++
			}
		}
	}
	cdw.WContext.SetValue(cycleCountValue, serialize.SInt64(cycleCount))
}

func (cdw *CycleDetectionWorker) OutputValueType() string {
	if cdw.mode == Boolean {
		return common.HgValueTypeInt
	}
	return common.HgValueTypeString
}

type CycleDetectionMaster struct {
	compute.MasterComputerBase
}

func (cdm *CycleDetectionMaster) Init() error {
	return nil
}

func (cdm *CycleDetectionMaster) Compute() bool {
	if cdm.MContext.Step == 2 {
		cycleNum := cdm.MContext.GetValue(cycleCountValue).(serialize.SInt64)
		logrus.Infof("cycles num: %v", cycleNum)
		return false
	}
	return true
}
