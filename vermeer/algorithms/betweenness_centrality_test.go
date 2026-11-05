/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with this
work for additional information regarding copyright ownership. The ASF
licenses this file to You under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package algorithms

import (
	"encoding/binary"
	"math"
	"testing"
	"vermeer/apps/buffer"
	"vermeer/apps/compute"
	"vermeer/apps/serialize"
)

func TestBrandesBetweennessPath(t *testing.T) {
	neighbors := []serialize.SliceUint32{{1}, {0, 2}, {1}}
	scale := betweennessScale(3, false, 1)
	values := make([]float64, 3)
	for source := uint32(0); source < 3; source++ {
		for vertex, value := range brandesContributions(source, neighbors, false, scale) {
			values[vertex] += value
		}
	}
	if math.Abs(values[1]-1) > 1e-6 {
		t.Fatalf("middle vertex expected normalized betweenness 1, got %v", values[1])
	}
	if values[0] != 0 || values[2] != 0 {
		t.Fatalf("path endpoints must have zero betweenness, got %v", values)
	}
}

func TestBrandesSplitsEqualShortestPaths(t *testing.T) {
	neighbors := []serialize.SliceUint32{{1, 2}, {3}, {3}, {}}
	values := brandesContributions(0, neighbors, false, 1)
	if math.Abs(values[1]-0.5) > 1e-6 ||
		math.Abs(values[2]-0.5) > 1e-6 {
		t.Fatalf("diamond paths were not split equally: %v", values)
	}
}

func TestBrandesPathCountsStayFiniteBeyondFloat64Range(t *testing.T) {
	const layers = 1030
	neighbors := make([]serialize.SliceUint32, 1+layers*2)
	neighbors[0] = serialize.SliceUint32{1, 2}
	for layer := 0; layer < layers-1; layer++ {
		left := uint32(1 + layer*2)
		right := left + 1
		nextLeft := left + 2
		nextRight := left + 3
		neighbors[left] = serialize.SliceUint32{serialize.SUint32(nextLeft),
			serialize.SUint32(nextRight)}
		neighbors[right] = serialize.SliceUint32{serialize.SUint32(nextLeft),
			serialize.SUint32(nextRight)}
	}

	values := brandesContributions(0, neighbors, false, 1)
	for vertex, value := range values {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			t.Fatalf("vertex %d has non-finite contribution %v", vertex, value)
		}
	}
	lastLeft := uint32(len(neighbors) - 2)
	lastRight := lastLeft + 1
	if math.Abs(values[lastLeft]-values[lastRight]) > 1e-9 {
		t.Fatalf("symmetric final layer differs: left=%v right=%v",
			values[lastLeft], values[lastRight])
	}
}

func TestBrandesDisconnectedSourcesDoNotAllocateGlobalStacks(t *testing.T) {
	const vertices = 4096
	neighbors := make([]serialize.SliceUint32, vertices)
	for source := uint32(0); source < vertices; source++ {
		if values := brandesContributions(source, neighbors, false, 1); len(values) != 0 {
			t.Fatalf("isolated source %d produced contributions: %v", source, values)
		}
	}
}

func TestBetweennessScaleAndValidation(t *testing.T) {
	if scale := betweennessScale(2, false, 1); scale != 0 {
		t.Fatalf("two-vertex non-endpoint normalization must be zero, got %v", scale)
	}
	if scale := betweennessScale(4, false, 0.5); math.Abs(scale-1.0/3.0) > 1e-12 {
		t.Fatalf("unexpected sampled scale: %v", scale)
	}
	for _, value := range []float64{0, -1, 1.1, math.NaN(), math.Inf(1)} {
		if err := validateBetweennessSampleRate(value); err == nil {
			t.Fatalf("invalid sample rate %v accepted", value)
		}
	}
	for _, value := range []int{-1, 2} {
		if _, err := parseBetweennessEndpoint(value); err == nil {
			t.Fatalf("invalid endpoint flag %d accepted", value)
		}
	}
	if err := validateBetweennessMaxStep(1); err == nil {
		t.Fatal("compute.max_step=1 was accepted")
	}
	if err := validateBetweennessMaxStep(2); err != nil {
		t.Fatalf("compute.max_step=2 was rejected: %v", err)
	}
}

func TestBetweennessSourceSamplingIsDeterministicAndCompacted(t *testing.T) {
	first := selectBetweennessSources(100, 10_000, 0.01)
	second := selectBetweennessSources(100, 10_000, 0.01)
	if len(first) == 0 || len(first) >= 10_000 {
		t.Fatalf("unexpected selected source count %d", len(first))
	}
	if len(first) != len(second) {
		t.Fatalf("deterministic selections differ in length: %d vs %d",
			len(first), len(second))
	}
	for index, source := range first {
		if source != second[index] {
			t.Fatalf("selection differs at %d: %d vs %d",
				index, source, second[index])
		}
		if !sampleBetweennessSource(source, 0.01) {
			t.Fatalf("unselected source %d was compacted into the work list", source)
		}
	}
}

func TestExactModeDoesNotRetainASecondSourceIDArray(t *testing.T) {
	worker := &BetweennessCentralityWorker{
		localStart:      50,
		localCount:      3,
		allSources:      true,
		selectedSources: 3,
	}
	if len(worker.sampledSources) != 0 {
		t.Fatalf("exact mode retained sampled source IDs: %v", worker.sampledSources)
	}
	for index, expected := range []uint32{50, 51, 52} {
		if got := worker.selectedSource(index); got != expected {
			t.Fatalf("source %d expected %d, got %d", index, expected, got)
		}
	}
}

func TestSplitBetweennessContributionsKeepsLocalAndChunksRemote(t *testing.T) {
	partial := []float64{0, 1.25, 2.5, 3.75}
	local, chunks, err := splitBetweennessContributions(partial, 1, 2)
	if err != nil {
		t.Fatalf("split contributions: %v", err)
	}
	if len(local) != 2 || local[0] != 1.25 || local[1] != 2.5 {
		t.Fatalf("unexpected local contribution slice: %v", local)
	}
	if len(chunks) != 1 {
		t.Fatalf("expected one remote chunk, got %d", len(chunks))
	}
	if count := binary.LittleEndian.Uint32(chunks[0].payload[:4]); count != 1 {
		t.Fatalf("expected one remote record, got %d", count)
	}

	remote := &BetweennessCentralityWorker{
		localStart:  3,
		localCount:  1,
		localValues: make([]float64, 1),
	}
	receiver := betweennessContributionReceiver{worker: remote}
	if _, err := receiver.Unmarshal(chunks[0].payload); err != nil {
		t.Fatalf("receive contribution chunk: %v", err)
	}
	if math.Abs(remote.localValues[0]-3.75) > 1e-6 {
		t.Fatalf("remote contribution expected 3.75, got %v", remote.localValues[0])
	}
}

func TestBetweennessContributionChunksStayBounded(t *testing.T) {
	partial := make([]float64, maxBetweennessChunkRecords+5)
	for index := range partial {
		partial[index] = float64(index + 1)
	}
	_, chunks, err := splitBetweennessContributions(partial, 0, 0)
	if err != nil {
		t.Fatalf("split bounded chunks: %v", err)
	}
	if len(chunks) != 2 {
		t.Fatalf("expected two chunks, got %d", len(chunks))
	}
	maximum := 4 + maxBetweennessChunkRecords*betweennessRecordSize
	for index, chunk := range chunks {
		if chunk.PredictSize() > maximum {
			t.Fatalf("chunk %d has %d bytes, maximum is %d",
				index, chunk.PredictSize(), maximum)
		}
		if chunk.PredictSize() >= 2*1024*1024 {
			t.Fatalf("chunk %d is not scatter-buffer bounded: %d bytes",
				index, chunk.PredictSize())
		}
	}
}

func TestBetweennessContributionChunkRejectsTruncation(t *testing.T) {
	payload := make([]byte, 4)
	binary.LittleEndian.PutUint32(payload, 1)
	var chunk betweennessContributionChunk
	if _, err := chunk.Unmarshal(payload); err == nil {
		t.Fatal("truncated contribution chunk was accepted")
	}

	worker := &BetweennessCentralityWorker{
		localCount:  1,
		localValues: make([]float64, 1),
	}
	receiver := betweennessContributionReceiver{worker: worker}
	if _, err := receiver.Unmarshal(payload); err == nil {
		t.Fatal("receiver accepted a truncated contribution chunk")
	}
}

func TestBetweennessInactiveScatterPayloadIsConstant(t *testing.T) {
	var encoded buffer.EncodeBuffer
	encoded.Init(1024)
	offset := serialize.SUint32(100)
	if err := encoded.Marshal(&offset); err != nil {
		t.Fatalf("marshal offset: %v", err)
	}
	empty := &betweennessContributionReceiver{}
	for index := 0; index < 10_000; index++ {
		if err := encoded.Marshal(empty); err != nil {
			t.Fatalf("marshal empty value %d: %v", index, err)
		}
	}
	if got := len(encoded.PayLoad()); got != 4 {
		t.Fatalf("10000 inactive vertices produced %d payload bytes, expected 4", got)
	}
}

func TestBetweennessOutgoingChunksUseLocalVertexWindows(t *testing.T) {
	worker := &BetweennessCentralityWorker{
		localStart:     10,
		localCount:     2,
		outgoingChunks: make([]betweennessContributionChunk, 5),
	}
	context := worker.MakeContext(nil)
	context.Step = 2
	context.CreateValue(betweennessStopValue, compute.ValueTypeInt32,
		compute.CValueActionAggregate)
	context.SetValue(betweennessStopValue, serialize.SInt32(1))
	worker.receiver.worker = worker

	cases := []struct {
		window int
		offset int
	}{
		{window: 2, offset: 2},
		{window: 2, offset: 4},
		{window: 1, offset: 5},
	}
	for _, test := range cases {
		if got := worker.outgoingWindowCount(); got != test.window {
			t.Fatalf("offset %d expected window %d, got %d",
				worker.outgoingOffset, test.window, got)
		}
		if _, ok := worker.VertexValue(10).(*betweennessContributionChunk); !ok {
			t.Fatalf("offset %d did not expose a bounded contribution chunk",
				worker.outgoingOffset)
		}
		worker.AfterStep()
		if worker.outgoingOffset != test.offset {
			t.Fatalf("expected outgoing offset %d, got %d",
				test.offset, worker.outgoingOffset)
		}
		context.Step++
	}
	if stop := context.GetValue(betweennessStopValue).(serialize.SInt32); stop != 0 {
		t.Fatalf("final chunk window did not stop: %d", stop)
	}
}

func TestBetweennessMasterCompletesOnlyAfterScatterDrains(t *testing.T) {
	master := &BetweennessCentralityMaster{}
	context := master.MakeContext(map[string]string{"compute.max_step": "10"})
	if err := master.Init(); err != nil {
		t.Fatalf("master init: %v", err)
	}
	context.CreateValue(betweennessStopValue, compute.ValueTypeInt32,
		compute.CValueActionAggregate)
	context.SetValue(betweennessStopValue, serialize.SInt32(1))
	if !master.Compute() {
		t.Fatal("master stopped while a worker still had contribution chunks")
	}
	if complete := context.GetValue(betweennessCompleteValue).(serialize.SInt32); complete != 0 {
		t.Fatalf("completion was published early: %d", complete)
	}

	context.SetValue(betweennessStopValue, serialize.SInt32(0))
	if master.Compute() {
		t.Fatal("master continued after every worker drained its chunks")
	}
	if complete := context.GetValue(betweennessCompleteValue).(serialize.SInt32); complete != 1 {
		t.Fatalf("completion was not published: %d", complete)
	}
}

func TestBetweennessOutputRequiresScalarCompletionAndUsesLocalRange(t *testing.T) {
	worker := &BetweennessCentralityWorker{
		localStart:  10,
		localCount:  2,
		localValues: []float64{1.5, 2.5},
	}
	context := worker.MakeContext(nil)
	context.CreateValue(betweennessCompleteValue, compute.ValueTypeInt32,
		compute.CValueActionNormal)
	context.SetValue(betweennessCompleteValue, serialize.SInt32(0))
	if err := worker.ValidateOutput(); err == nil {
		t.Fatal("incomplete scalar state was accepted")
	}

	context.SetValue(betweennessCompleteValue, serialize.SInt32(1))
	if err := worker.ValidateOutput(); err != nil {
		t.Fatalf("completed local output rejected: %v", err)
	}
	context.Output = true
	for vertexID, expected := range map[uint32]float32{10: 1.5, 11: 2.5} {
		value, ok := worker.VertexValue(vertexID).(*serialize.SFloat32)
		if !ok {
			t.Fatalf("vertex %d output type is %T", vertexID, worker.VertexValue(vertexID))
		}
		if float32(*value) != expected {
			t.Fatalf("vertex %d expected %v, got %v", vertexID, expected, *value)
		}
	}
}
