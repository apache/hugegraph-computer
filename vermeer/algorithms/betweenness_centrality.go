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
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"vermeer/apps/common"
	"vermeer/apps/compute"
	"vermeer/apps/options"
	"vermeer/apps/serialize"

	"github.com/sirupsen/logrus"
)

const (
	betweennessStopValue     = "stop_sum"
	betweennessCompleteValue = "betweenness_complete"

	// A contribution chunk stays well below the 2 MiB scatter buffer. Several
	// chunks can therefore be sent in one bounded RPC frame, while no result ever
	// becomes a whole-graph compute value or a single multi-gigabyte message.
	maxBetweennessChunkRecords = 65_536
	betweennessRecordSize      = 8 // uint32 target + float32 worker partial
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

func validateBetweennessMaxStep(maxStep int) error {
	if maxStep < 2 {
		return fmt.Errorf("betweenness_centrality requires compute.max_step >= 2, got %d",
			maxStep)
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

func selectBetweennessSources(start, count uint32, sampleRate float64) []uint32 {
	if count == 0 {
		return nil
	}
	capacity := int(math.Ceil(float64(count) * sampleRate))
	if capacity < 1 {
		capacity = 1
	}
	sources := make([]uint32, 0, capacity)
	for localID := uint32(0); localID < count; localID++ {
		vertexID := start + localID
		if sampleBetweennessSource(vertexID, sampleRate) {
			sources = append(sources, vertexID)
		}
	}
	return sources
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

// betweennessContributionChunk is already encoded so its retained memory is
// exactly the wire representation: a uint32 count followed by count pairs of
// (uint32 target, float32 worker partial). It never contains a whole-graph
// vector and each chunk has a fixed upper bound.
type betweennessContributionChunk struct {
	payload []byte
}

func (chunk *betweennessContributionChunk) Marshal(buffer []byte) (int, error) {
	if len(buffer) < len(chunk.payload) {
		return 0, fmt.Errorf("betweenness contribution buffer has %d bytes, need %d",
			len(buffer), len(chunk.payload))
	}
	return copy(buffer, chunk.payload), nil
}

func (chunk *betweennessContributionChunk) Unmarshal(buffer []byte) (int, error) {
	size, err := betweennessChunkSize(buffer)
	if err != nil {
		return 0, err
	}
	chunk.payload = append(chunk.payload[:0], buffer[:size]...)
	return size, nil
}

func (chunk *betweennessContributionChunk) ToString() string {
	if len(chunk.payload) < 4 {
		return "0"
	}
	return fmt.Sprintf("%d", binary.LittleEndian.Uint32(chunk.payload[:4]))
}

func (chunk *betweennessContributionChunk) PredictSize() int {
	return len(chunk.payload)
}

func betweennessChunkSize(buffer []byte) (int, error) {
	if len(buffer) < 4 {
		return 0, fmt.Errorf("betweenness contribution chunk is missing its count")
	}
	count := uint64(binary.LittleEndian.Uint32(buffer[:4]))
	size := uint64(4) + count*betweennessRecordSize
	if size > uint64(len(buffer)) {
		return 0, fmt.Errorf("truncated betweenness contribution chunk: need %d bytes, got %d",
			size, len(buffer))
	}
	return int(size), nil
}

func appendBetweennessRecord(payload []byte, target uint32, value float32) []byte {
	offset := len(payload)
	payload = append(payload, make([]byte, betweennessRecordSize)...)
	binary.LittleEndian.PutUint32(payload[offset:offset+4], target)
	binary.LittleEndian.PutUint32(payload[offset+4:offset+8], math.Float32bits(value))
	return payload
}

func splitBetweennessContributions(partial []float64, localStart,
	localCount uint32) ([]float64, []betweennessContributionChunk, error) {
	local := make([]float64, localCount)
	localEnd := uint64(localStart) + uint64(localCount)
	chunks := make([]betweennessContributionChunk, 0)
	var payload []byte
	records := 0

	flush := func() {
		if records == 0 {
			return
		}
		binary.LittleEndian.PutUint32(payload[:4], uint32(records))
		chunks = append(chunks, betweennessContributionChunk{payload: payload})
		payload = nil
		records = 0
	}

	for target, value := range partial {
		if value == 0 {
			continue
		}
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, nil, fmt.Errorf("betweenness partial for vertex %d is not finite", target)
		}
		rounded := float32(value)
		if math.IsInf(float64(rounded), 0) {
			return nil, nil, fmt.Errorf("betweenness partial for vertex %d exceeds float32", target)
		}
		if rounded == 0 {
			continue
		}

		targetID := uint32(target)
		if uint64(targetID) >= uint64(localStart) && uint64(targetID) < localEnd {
			local[targetID-localStart] += float64(rounded)
			continue
		}
		if records == 0 {
			payload = make([]byte, 4, 4+maxBetweennessChunkRecords*betweennessRecordSize)
		}
		payload = appendBetweennessRecord(payload, targetID, rounded)
		records++
		if records == maxBetweennessChunkRecords {
			flush()
		}
	}
	flush()
	return local, chunks, nil
}

type betweennessContributionReceiver struct {
	worker *BetweennessCentralityWorker
}

func (*betweennessContributionReceiver) Marshal([]byte) (int, error) {
	// Inactive local vertex slots intentionally contribute no bytes. Active
	// slots return a betweennessContributionChunk from VertexValue instead.
	return 0, nil
}

func (receiver *betweennessContributionReceiver) Unmarshal(buffer []byte) (int, error) {
	if receiver.worker == nil {
		return 0, fmt.Errorf("betweenness contribution receiver is not initialized")
	}
	size, err := betweennessChunkSize(buffer)
	if err != nil {
		return 0, err
	}
	count := int(binary.LittleEndian.Uint32(buffer[:4]))
	worker := receiver.worker
	localEnd := uint64(worker.localStart) + uint64(worker.localCount)

	worker.valuesMu.Lock()
	defer worker.valuesMu.Unlock()
	for index := 0; index < count; index++ {
		offset := 4 + index*betweennessRecordSize
		target := binary.LittleEndian.Uint32(buffer[offset : offset+4])
		value := math.Float32frombits(binary.LittleEndian.Uint32(
			buffer[offset+4 : offset+8]))
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return 0, fmt.Errorf("received non-finite betweenness value for vertex %d", target)
		}
		if uint64(target) < uint64(worker.localStart) || uint64(target) >= localEnd {
			continue
		}
		worker.localValues[target-worker.localStart] += float64(value)
	}
	return size, nil
}

func (*betweennessContributionReceiver) ToString() string {
	return ""
}

func (*betweennessContributionReceiver) PredictSize() int {
	return 0
}

type BetweennessCentralityWorker struct {
	compute.WorkerComputerBase
	useEndPoint       bool
	scale             float64
	sampleRate        float64
	totalVertices     uint32
	localStart        uint32
	localCount        uint32
	sampledSources    []uint32
	allSources        bool
	selectedSources   int
	neighborEdges     []serialize.SliceUint32
	localValues       []float64
	betweennessValues serialize.SliceFloat32
	outgoingChunks    []betweennessContributionChunk
	outgoingOffset    int
	computed          bool
	valuesMu          sync.Mutex
	receiver          betweennessContributionReceiver
}

func (bcw *BetweennessCentralityWorker) VertexValue(vertexID uint32) serialize.MarshalAble {
	if bcw.WContext.Output {
		if vertexID < bcw.localStart || vertexID >= bcw.localStart+bcw.localCount {
			return &bcw.receiver
		}
		return &bcw.betweennessValues[vertexID-bcw.localStart]
	}
	if bcw.WContext.Step == 1 {
		return &bcw.neighborEdges[vertexID]
	}

	// The standard scatter loop provides one slot per local vertex. Fill the
	// current window with bounded contribution chunks and leave every remaining
	// slot at zero bytes. Receivers do not rely on the slot's vertex ID because
	// each record carries its actual target ID.
	if vertexID >= bcw.localStart && vertexID < bcw.localStart+bcw.localCount {
		localID := int(vertexID - bcw.localStart)
		window := bcw.outgoingWindowCount()
		if localID < window {
			return &bcw.outgoingChunks[bcw.outgoingOffset+localID]
		}
	}
	return &bcw.receiver
}

func (bcw *BetweennessCentralityWorker) ValidateOutput() error {
	cValue, ok := bcw.WContext.CValues[betweennessCompleteValue]
	if !ok || cValue.Value == nil {
		return fmt.Errorf("betweenness_centrality output is missing completion state")
	}
	complete, ok := cValue.Value.(serialize.SInt32)
	if !ok || complete != 1 {
		return fmt.Errorf("betweenness_centrality did not finish contribution scattering; increase compute.max_step")
	}
	if uint32(len(bcw.localValues)) != bcw.localCount {
		return fmt.Errorf("betweenness_centrality local result has %d values, expected %d",
			len(bcw.localValues), bcw.localCount)
	}
	bcw.betweennessValues = make(serialize.SliceFloat32, bcw.localCount)
	for localID, value := range bcw.localValues {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return fmt.Errorf("betweenness result for local vertex %d is not finite", localID)
		}
		bcw.betweennessValues[localID] = serialize.SFloat32(value)
	}
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
	if err := validateBetweennessMaxStep(options.GetInt(bcw.WContext.Params,
		"compute.max_step")); err != nil {
		return err
	}

	bcw.useEndPoint = useEndpoint
	bcw.totalVertices = bcw.WContext.GraphData.Vertex.TotalVertexCount()
	bcw.localStart = bcw.WContext.GraphData.VertIDStart
	bcw.localCount = bcw.WContext.GraphData.VertexCount
	bcw.scale = betweennessScale(bcw.totalVertices, bcw.useEndPoint, bcw.sampleRate)
	if bcw.sampleRate >= 1 {
		bcw.allSources = true
		bcw.selectedSources = int(bcw.localCount)
	} else {
		bcw.sampledSources = selectBetweennessSources(bcw.localStart, bcw.localCount,
			bcw.sampleRate)
		bcw.selectedSources = len(bcw.sampledSources)
	}
	bcw.neighborEdges = make([]serialize.SliceUint32, bcw.totalVertices)
	bcw.localValues = make([]float64, bcw.localCount)
	bcw.betweennessValues = make(serialize.SliceFloat32, bcw.localCount)
	bcw.receiver.worker = bcw

	bcw.WContext.CreateValue(betweennessStopValue, compute.ValueTypeInt32,
		compute.CValueActionAggregate)
	bcw.WContext.SetValue(betweennessStopValue, serialize.SInt32(1))
	bcw.WContext.CreateValue(betweennessCompleteValue, compute.ValueTypeInt32,
		compute.CValueActionNormal)
	bcw.WContext.SetValue(betweennessCompleteValue, serialize.SInt32(0))
	return nil
}

func (bcw *BetweennessCentralityWorker) BeforeStep() {
	if bcw.WContext.Step != 2 || bcw.computed {
		return
	}
	partial := bcw.computeSampledSourcePartials()
	local, chunks, err := splitBetweennessContributions(partial,
		bcw.localStart, bcw.localCount)
	if err != nil {
		panic(err)
	}
	bcw.localValues = local
	bcw.outgoingChunks = chunks
	bcw.outgoingOffset = 0
	bcw.computed = true
	logrus.Infof("betweenness selected sources: %d, contribution chunks: %d",
		bcw.selectedSources, len(bcw.outgoingChunks))
}

func (bcw *BetweennessCentralityWorker) Compute(vertexID uint32, _ int) {
	if bcw.WContext.Step != 1 {
		return
	}
	localID := vertexID - bcw.localStart
	inEdges := bcw.WContext.GraphData.Edges.GetInEdges(localID)
	neighbors := make(serialize.SliceUint32, 0, len(inEdges))
	seen := make(map[serialize.SUint32]struct{}, len(inEdges))
	for _, neighbor := range inEdges {
		if uint32(neighbor) == vertexID || uint32(neighbor) >= bcw.totalVertices {
			continue
		}
		if _, exists := seen[neighbor]; exists {
			continue
		}
		seen[neighbor] = struct{}{}
		neighbors = append(neighbors, neighbor)
	}
	bcw.neighborEdges[vertexID] = neighbors
}

func (bcw *BetweennessCentralityWorker) AfterStep() {
	if bcw.WContext.Step == 1 {
		if bcw.selectedSources == 0 || bcw.scale == 0 {
			bcw.WContext.SetValue(betweennessStopValue, serialize.SInt32(0))
		} else {
			bcw.WContext.SetValue(betweennessStopValue, serialize.SInt32(1))
		}
		return
	}

	bcw.outgoingOffset += bcw.outgoingWindowCount()
	if bcw.outgoingOffset < len(bcw.outgoingChunks) {
		bcw.WContext.SetValue(betweennessStopValue, serialize.SInt32(1))
		return
	}
	bcw.outgoingChunks = nil
	bcw.WContext.SetValue(betweennessStopValue, serialize.SInt32(0))
}

func (bcw *BetweennessCentralityWorker) outgoingWindowCount() int {
	remaining := len(bcw.outgoingChunks) - bcw.outgoingOffset
	if remaining <= 0 || bcw.localCount == 0 {
		return 0
	}
	if remaining < int(bcw.localCount) {
		return remaining
	}
	return int(bcw.localCount)
}

func (bcw *BetweennessCentralityWorker) computeSampledSourcePartials() []float64 {
	partial := make([]float64, bcw.totalVertices)
	if bcw.selectedSources == 0 || bcw.scale == 0 {
		return partial
	}
	parallel := bcw.WContext.Parallel
	if parallel <= 0 {
		parallel = 1
	}
	if parallel > bcw.selectedSources {
		parallel = bcw.selectedSources
	}

	results := make(chan map[uint32]float64, parallel)
	var workers sync.WaitGroup
	workers.Add(parallel)
	for workerID := 0; workerID < parallel; workerID++ {
		go func(offset int) {
			defer workers.Done()
			for sourceIndex := offset; sourceIndex < bcw.selectedSources; sourceIndex += parallel {
				results <- brandesContributions(bcw.selectedSource(sourceIndex),
					bcw.neighborEdges, bcw.useEndPoint, bcw.scale)
			}
		}(workerID)
	}
	go func() {
		workers.Wait()
		close(results)
	}()

	for contributions := range results {
		for target, value := range contributions {
			partial[target] += value
		}
	}
	return partial
}

func (bcw *BetweennessCentralityWorker) selectedSource(index int) uint32 {
	if bcw.allSources {
		return bcw.localStart + uint32(index)
	}
	return bcw.sampledSources[index]
}

func (bcw *BetweennessCentralityWorker) OutputValueType() string {
	return common.HgValueTypeFloat
}

type BetweennessCentralityMaster struct {
	compute.MasterComputerBase
}

func (bcm *BetweennessCentralityMaster) Init() error {
	if err := validateBetweennessMaxStep(options.GetInt(bcm.MContext.Params,
		"compute.max_step")); err != nil {
		return err
	}
	bcm.MContext.CreateValue(betweennessCompleteValue, compute.ValueTypeInt32,
		compute.CValueActionNormal)
	bcm.MContext.SetValue(betweennessCompleteValue, serialize.SInt32(0))
	return nil
}

func (bcm *BetweennessCentralityMaster) Compute() bool {
	stopSum := bcm.MContext.GetValue(betweennessStopValue).(serialize.SInt32)
	if stopSum != 0 {
		return true
	}
	bcm.MContext.SetValue(betweennessCompleteValue, serialize.SInt32(1))
	return false
}
