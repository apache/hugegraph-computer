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
	"vermeer/apps/common"
	"vermeer/apps/compute"
	"vermeer/apps/options"
	"vermeer/apps/serialize"

	"github.com/sirupsen/logrus"
)

const (
	betweennessStopValue        = "stop_sum"
	betweennessLocalCountsValue = "betweenness_local_counts"
	betweennessBatchCountValue  = "betweenness_batch_count"
	betweennessPartialValue     = "betweenness_partial_values"
	betweennessFinalValue       = "betweenness_final_values"
)

func init() {
	Algorithms = append(Algorithms, new(BetweennessCentralityMaker))
}

type BetweennessCentralityMaker struct {
	compute.AlgorithmMakerBase
}

func (bc *BetweennessCentralityMaker) CreateWorkerComputer() compute.WorkerComputer {
	return &BetweennessCentralityWorker{}
}

func (bc *BetweennessCentralityMaker) CreateMasterComputer() compute.MasterComputer {
	return &BetweennessCentralityMaster{}
}

func (bc *BetweennessCentralityMaker) Name() string {
	return "betweenness_centrality"
}

func (bc *BetweennessCentralityMaker) DataNeeded() []string {
	return []string{}
}

func validateBetweennessSampleRate(value float64) error {
	if math.IsNaN(value) || math.IsInf(value, 0) || value <= 0 || value > 1 {
		return fmt.Errorf("betweenness_centrality.sample_rate must be finite and in (0, 1], got %v",
			value)
	}
	return nil
}

func parseBetweennessEndpoint(value int) (bool, error) {
	switch value {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, fmt.Errorf("betweenness_centrality.use_endpoint must be 0 or 1, got %d",
			value)
	}
}

func betweennessScale(totalVertices uint32, useEndpoint bool, sampleRate float64) float64 {
	if totalVertices <= 1 {
		return 0
	}
	n := float64(totalVertices)
	var scale float64
	if useEndpoint {
		scale = 1 / (n * (n - 1))
	} else {
		if totalVertices <= 2 {
			return 0
		}
		scale = 1 / ((n - 1) * (n - 2))
	}
	return scale / sampleRate
}

func planBetweennessBatches(maxLocalVertices uint32, parallel, maxStep int) (int, error) {
	if maxLocalVertices == 0 {
		return 0, nil
	}
	if maxStep < 2 {
		return 0, fmt.Errorf("betweenness_centrality requires compute.max_step >= 2, got %d",
			maxStep)
	}
	if parallel <= 0 {
		parallel = 1
	}
	ideal := (uint64(maxLocalVertices) + uint64(parallel) - 1) / uint64(parallel)
	available := uint64(maxStep - 1)
	if ideal > available {
		ideal = available
	}
	return int(ideal), nil
}

func collectBetweennessPartitionCounts(
	workerValues map[string]map[string]*compute.CValue) (serialize.SliceInt64, error) {
	counts := make(serialize.SliceInt64, 0, len(workerValues))
	for workerName, values := range workerValues {
		cValue, ok := values[betweennessLocalCountsValue]
		if !ok || cValue.Value == nil {
			return nil, fmt.Errorf("worker %s did not report its betweenness partition size",
				workerName)
		}
		count, ok := cValue.Value.(serialize.SInt64)
		if !ok {
			return nil, fmt.Errorf("worker %s reported invalid betweenness partition type %T",
				workerName, cValue.Value)
		}
		counts = append(counts, count)
	}
	return counts, nil
}

func summarizeBetweennessPartitions(counts serialize.SliceInt64) (uint32, uint32, error) {
	var maxLocal uint64
	var total uint64
	for _, rawCount := range counts {
		count := int64(rawCount)
		if count < 0 {
			return 0, 0, fmt.Errorf("negative betweenness partition size %d", count)
		}
		value := uint64(count)
		total += value
		if value > maxLocal {
			maxLocal = value
		}
	}
	if total > uint64(^uint32(0)) || maxLocal > uint64(^uint32(0)) {
		return 0, 0, fmt.Errorf("betweenness partition sizes exceed uint32 vertex range")
	}
	return uint32(maxLocal), uint32(total), nil
}

func sampleBetweennessSource(vertexID uint32, sampleRate float64) bool {
	if sampleRate >= 1 {
		return true
	}
	value := uint64(vertexID) + 0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	value ^= value >> 31
	return float64(value>>11) < sampleRate*float64(uint64(1)<<53)
}

func logAddExp(left, right float64) float64 {
	if math.IsInf(left, -1) {
		return right
	}
	if math.IsInf(right, -1) {
		return left
	}
	if left < right {
		left, right = right, left
	}
	return left + math.Log1p(math.Exp(right-left))
}

func brandesContributions(source uint32, neighbors []serialize.SliceUint32,
	useEndpoint bool, scale float64) map[uint32]float64 {
	result := make(map[uint32]float64)
	if scale == 0 || int(source) >= len(neighbors) {
		return result
	}

	predecessors := make(map[uint32][]uint32)
	logSigma := map[uint32]float64{source: 0}
	distance := map[uint32]int{source: 0}
	queue := []uint32{source}

	for head := 0; head < len(queue); head++ {
		vertex := queue[head]
		nextDistance := distance[vertex] + 1
		for _, rawNeighbor := range neighbors[vertex] {
			neighbor := uint32(rawNeighbor)
			if int(neighbor) >= len(neighbors) {
				continue
			}
			neighborDistance, seen := distance[neighbor]
			if !seen {
				neighborDistance = nextDistance
				distance[neighbor] = neighborDistance
				queue = append(queue, neighbor)
			}
			if neighborDistance != nextDistance {
				continue
			}
			currentLogSigma, ok := logSigma[neighbor]
			if !ok {
				currentLogSigma = math.Inf(-1)
			}
			logSigma[neighbor] = logAddExp(currentLogSigma, logSigma[vertex])
			predecessors[neighbor] = append(predecessors[neighbor], vertex)
		}
	}

	dependency := make(map[uint32]float64, len(queue))
	if useEndpoint && len(queue) > 1 {
		result[source] = float64(len(queue)-1) * scale
	}
	for index := len(queue) - 1; index >= 0; index-- {
		vertex := queue[index]
		for _, predecessor := range predecessors[vertex] {
			pathRatio := math.Exp(logSigma[predecessor] - logSigma[vertex])
			dependency[predecessor] += pathRatio * (1 + dependency[vertex])
		}
		if vertex == source {
			continue
		}
		contribution := dependency[vertex]
		if useEndpoint {
			contribution++
		}
		contribution *= scale
		if contribution != 0 {
			result[vertex] = contribution
		}
	}
	return result
}

func sumBetweennessPartials(partials []serialize.SliceFloat32,
	totalVertices uint32) (serialize.SliceFloat32, error) {
	if totalVertices == 0 {
		return serialize.SliceFloat32{}, nil
	}
	if len(partials) == 0 {
		return nil, fmt.Errorf("no betweenness worker partials were reported")
	}

	sums := make([]float64, totalVertices)
	for workerIndex, partial := range partials {
		if uint32(len(partial)) != totalVertices {
			return nil, fmt.Errorf("betweenness worker %d reported %d values, expected %d",
				workerIndex, len(partial), totalVertices)
		}
		for vertexID, value := range partial {
			sums[vertexID] += float64(value)
		}
	}

	result := make(serialize.SliceFloat32, totalVertices)
	for vertexID, value := range sums {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, fmt.Errorf("betweenness result for vertex %d is not finite", vertexID)
		}
		result[vertexID] = serialize.SFloat32(value)
	}
	return result, nil
}

type emptyBetweennessVertexValue struct{}

func (*emptyBetweennessVertexValue) Marshal([]byte) (int, error) {
	return 0, nil
}

func (*emptyBetweennessVertexValue) Unmarshal([]byte) (int, error) {
	return 0, nil
}

func (*emptyBetweennessVertexValue) ToString() string {
	return ""
}

func (*emptyBetweennessVertexValue) PredictSize() int {
	return 0
}

var noBetweennessVertexValue emptyBetweennessVertexValue

type BetweennessCentralityWorker struct {
	compute.WorkerComputerBase
	useEndPoint       bool
	scale             float64
	sampleRate        float64
	maxStep           int
	batchCount        int
	currentBatch      int
	totalVertices     uint32
	betweennessValues serialize.SliceFloat32
	neighborEdges     []serialize.SliceUint32
	localValues       []float64
	batchValues       []map[uint32]float64
}

func (bcw *BetweennessCentralityWorker) VertexValue(vertexID uint32) serialize.MarshalAble {
	if bcw.WContext.Output {
		return &bcw.betweennessValues[vertexID]
	}
	if bcw.WContext.Step == 1 {
		return &bcw.neighborEdges[vertexID]
	}
	// The framework still visits every local vertex while scattering. Returning a
	// zero-byte value keeps the per-batch wire payload constant after adjacency
	// has been exchanged in step 1.
	return &noBetweennessVertexValue
}

func (bcw *BetweennessCentralityWorker) ValidateOutput() error {
	cValue, ok := bcw.WContext.CValues[betweennessFinalValue]
	if !ok || cValue.Value == nil {
		return fmt.Errorf("betweenness_centrality output is missing the global result")
	}
	values, ok := cValue.Value.(serialize.SliceFloat32)
	if !ok {
		return fmt.Errorf("betweenness_centrality output has invalid global result type %T",
			cValue.Value)
	}
	if uint32(len(values)) != bcw.totalVertices {
		return fmt.Errorf("betweenness_centrality did not finish all source batches: got %d global values, expected %d; increase compute.max_step",
			len(values), bcw.totalVertices)
	}
	bcw.betweennessValues = values
	return nil
}

func (bcw *BetweennessCentralityWorker) Init() error {
	bcw.sampleRate = options.GetFloat(bcw.WContext.Params,
		"betweenness_centrality.sample_rate")
	if err := validateBetweennessSampleRate(bcw.sampleRate); err != nil {
		return err
	}
	useEndpoint, err := parseBetweennessEndpoint(options.GetInt(bcw.WContext.Params,
		"betweenness_centrality.use_endpoint"))
	if err != nil {
		return err
	}
	bcw.maxStep = options.GetInt(bcw.WContext.Params, "compute.max_step")
	if bcw.maxStep < 2 {
		return fmt.Errorf("betweenness_centrality requires compute.max_step >= 2, got %d",
			bcw.maxStep)
	}

	bcw.useEndPoint = useEndpoint
	bcw.totalVertices = bcw.WContext.GraphData.Vertex.TotalVertexCount()
	localVertices := bcw.WContext.GraphData.VertexCount
	bcw.scale = betweennessScale(bcw.totalVertices, bcw.useEndPoint, bcw.sampleRate)
	bcw.betweennessValues = make(serialize.SliceFloat32, bcw.totalVertices)
	bcw.neighborEdges = make([]serialize.SliceUint32, bcw.totalVertices)
	bcw.localValues = make([]float64, bcw.totalVertices)
	bcw.batchValues = make([]map[uint32]float64, bcw.WContext.Parallel)

	bcw.WContext.CreateValue(betweennessStopValue, compute.ValueTypeInt32,
		compute.CValueActionAggregate)
	bcw.WContext.SetValue(betweennessStopValue, serialize.SInt32(1))
	// This value is intentionally Normal rather than Aggregate. Aggregate values
	// are broadcast back to workers; feeding the global slice into the next round
	// would multiply it by the worker count on every superstep.
	bcw.WContext.CreateValue(betweennessLocalCountsValue, compute.ValueTypeInt64,
		compute.CValueActionNormal)
	bcw.WContext.SetValue(betweennessLocalCountsValue, serialize.SInt64(localVertices))
	bcw.WContext.CreateValue(betweennessBatchCountValue, compute.ValueTypeInt32,
		compute.CValueActionNormal)
	bcw.WContext.SetValue(betweennessBatchCountValue, serialize.SInt32(0))
	bcw.WContext.CreateValue(betweennessPartialValue, compute.ValueTypeSliceFloat32,
		compute.CValueActionNormal)
	bcw.WContext.SetValue(betweennessPartialValue, serialize.SliceFloat32{})
	bcw.WContext.CreateValue(betweennessFinalValue, compute.ValueTypeSliceFloat32,
		compute.CValueActionNormal)
	bcw.WContext.SetValue(betweennessFinalValue, serialize.SliceFloat32{})
	return nil
}

func (bcw *BetweennessCentralityWorker) BeforeStep() {
	if bcw.WContext.Step < 2 {
		return
	}
	bcw.batchCount = int(bcw.WContext.GetValue(betweennessBatchCountValue).(serialize.SInt32))
	bcw.currentBatch = int(bcw.WContext.Step) - 2
	for partitionID := range bcw.batchValues {
		bcw.batchValues[partitionID] = make(map[uint32]float64)
	}
}

func (bcw *BetweennessCentralityWorker) Compute(vertexID uint32, partitionID int) {
	localID := vertexID - bcw.WContext.GraphData.VertIDStart
	if bcw.WContext.Step == 1 {
		inEdges := bcw.WContext.GraphData.Edges.GetInEdges(localID)
		neighbors := make(serialize.SliceUint32, 0, len(inEdges))
		seen := make(map[serialize.SUint32]struct{}, len(inEdges))
		for _, neighbor := range inEdges {
			if uint32(neighbor) == vertexID {
				continue
			}
			if _, exists := seen[neighbor]; exists {
				continue
			}
			seen[neighbor] = struct{}{}
			neighbors = append(neighbors, neighbor)
		}
		bcw.neighborEdges[vertexID] = neighbors
		return
	}

	if bcw.batchCount <= 0 || bcw.currentBatch >= bcw.batchCount ||
		int(localID)%bcw.batchCount != bcw.currentBatch {
		return
	}
	if !sampleBetweennessSource(vertexID, bcw.sampleRate) {
		return
	}

	contributions := brandesContributions(vertexID, bcw.neighborEdges,
		bcw.useEndPoint, bcw.scale)
	batch := bcw.batchValues[partitionID]
	for target, value := range contributions {
		batch[target] += value
	}
}

func (bcw *BetweennessCentralityWorker) AfterStep() {
	if bcw.WContext.Step == 1 {
		bcw.WContext.SetValue(betweennessStopValue, serialize.SInt32(1))
		return
	}

	bcw.aggregateCurrentBatch()
	if bcw.batchCount <= 0 || bcw.currentBatch+1 < bcw.batchCount {
		bcw.WContext.SetValue(betweennessStopValue, serialize.SInt32(1))
		return
	}

	partial := make(serialize.SliceFloat32, bcw.totalVertices)
	for vertexID, value := range bcw.localValues {
		partial[vertexID] = serialize.SFloat32(value)
	}
	bcw.WContext.SetValue(betweennessPartialValue, partial)
	bcw.WContext.SetValue(betweennessStopValue, serialize.SInt32(0))
}

func (bcw *BetweennessCentralityWorker) aggregateCurrentBatch() {
	for partitionID, values := range bcw.batchValues {
		for target, value := range values {
			bcw.localValues[target] += value
		}
		bcw.batchValues[partitionID] = nil
	}
}

func (bcw *BetweennessCentralityWorker) OutputValueType() string {
	return common.HgValueTypeFloat
}

type BetweennessCentralityMaster struct {
	compute.MasterComputerBase
	planned       bool
	totalVertices uint32
	batchCount    int
}

func (bcm *BetweennessCentralityMaster) Init() error {
	maxStep := options.GetInt(bcm.MContext.Params, "compute.max_step")
	if maxStep < 2 {
		return fmt.Errorf("betweenness_centrality requires compute.max_step >= 2, got %d",
			maxStep)
	}
	bcm.MContext.CreateValue(betweennessBatchCountValue, compute.ValueTypeInt32,
		compute.CValueActionNormal)
	bcm.MContext.SetValue(betweennessBatchCountValue, serialize.SInt32(0))
	bcm.MContext.CreateValue(betweennessFinalValue, compute.ValueTypeSliceFloat32,
		compute.CValueActionNormal)
	bcm.MContext.SetValue(betweennessFinalValue, serialize.SliceFloat32{})
	return nil
}

func (bcm *BetweennessCentralityMaster) Compute() bool {
	if !bcm.planned {
		counts, err := collectBetweennessPartitionCounts(bcm.MContext.WorkerCValues)
		if err != nil {
			logrus.Errorf("collect betweenness partition sizes: %v", err)
			return false
		}
		maxLocal, totalVertices, err := summarizeBetweennessPartitions(counts)
		if err != nil {
			logrus.Errorf("invalid betweenness partition summary: %v", err)
			return false
		}
		bcm.totalVertices = totalVertices
		bcm.batchCount, err = planBetweennessBatches(maxLocal,
			options.GetInt(bcm.MContext.Params, "compute.parallel"),
			options.GetInt(bcm.MContext.Params, "compute.max_step"))
		if err != nil {
			logrus.Errorf("plan betweenness batches: %v", err)
			return false
		}
		bcm.MContext.SetValue(betweennessBatchCountValue,
			serialize.SInt32(bcm.batchCount))
		bcm.planned = true
		logrus.Infof("betweenness source batches: %d, largest partition: %d",
			bcm.batchCount, maxLocal)
		if bcm.totalVertices == 0 {
			bcm.MContext.SetValue(betweennessFinalValue, serialize.SliceFloat32{})
			return false
		}
		return true
	}

	stopSum := bcm.MContext.GetValue(betweennessStopValue).(serialize.SInt32)
	if stopSum != 0 {
		return true
	}

	partials := make([]serialize.SliceFloat32, 0, len(bcm.MContext.WorkerCValues))
	for workerName, values := range bcm.MContext.WorkerCValues {
		cValue, ok := values[betweennessPartialValue]
		if !ok {
			logrus.Errorf("worker %s did not report betweenness partial values", workerName)
			bcm.MContext.SetValue(betweennessFinalValue, serialize.SliceFloat32{})
			return false
		}
		partial, ok := cValue.Value.(serialize.SliceFloat32)
		if !ok {
			logrus.Errorf("worker %s reported invalid betweenness partial type %T",
				workerName, cValue.Value)
			bcm.MContext.SetValue(betweennessFinalValue, serialize.SliceFloat32{})
			return false
		}
		partials = append(partials, partial)
	}
	result, err := sumBetweennessPartials(partials, bcm.totalVertices)
	if err != nil {
		logrus.Errorf("aggregate betweenness partial values: %v", err)
		bcm.MContext.SetValue(betweennessFinalValue, serialize.SliceFloat32{})
		return false
	}
	bcm.MContext.SetValue(betweennessFinalValue, result)
	return false
}
