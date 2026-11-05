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
}

func TestBetweennessPartitionCountsStayWorkerLocal(t *testing.T) {
	workerValues := map[string]map[string]*compute.CValue{
		"worker-a": {
			betweennessLocalCountsValue: {
				ValueType: compute.ValueTypeInt64,
				Action:    compute.CValueActionNormal,
				Value:     serialize.SInt64(9),
			},
		},
		"worker-b": {
			betweennessLocalCountsValue: {
				ValueType: compute.ValueTypeInt64,
				Action:    compute.CValueActionNormal,
				Value:     serialize.SInt64(1),
			},
		},
	}
	counts, err := collectBetweennessPartitionCounts(workerValues)
	if err != nil {
		t.Fatalf("collect partition counts: %v", err)
	}
	maxLocal, total, err := summarizeBetweennessPartitions(counts)
	if err != nil {
		t.Fatalf("summarize partition counts: %v", err)
	}
	if maxLocal != 9 || total != 10 || len(counts) != 2 {
		t.Fatalf("unexpected worker-local counts %v: max=%d total=%d",
			counts, maxLocal, total)
	}

	workerValues["worker-b"][betweennessLocalCountsValue].Value =
		serialize.SliceInt64{9, 1}
	if _, err := collectBetweennessPartitionCounts(workerValues); err == nil {
		t.Fatal("a broadcast aggregate slice was accepted as a local partition size")
	}
}

func TestBetweennessBatchPlanUsesLargestActualPartition(t *testing.T) {
	maxLocal, total, err := summarizeBetweennessPartitions(
		serialize.SliceInt64{9, 1, 1})
	if err != nil {
		t.Fatalf("partition summary failed: %v", err)
	}
	if maxLocal != 9 || total != 11 {
		t.Fatalf("unexpected partition summary: max=%d total=%d", maxLocal, total)
	}

	batches, err := planBetweennessBatches(maxLocal, 2, 10)
	if err != nil {
		t.Fatalf("batch plan failed: %v", err)
	}
	if batches != 5 {
		t.Fatalf("expected 5 batches for largest partition 9 at parallelism 2, got %d",
			batches)
	}
	counts := make([]int, batches)
	for localID := 0; localID < int(maxLocal); localID++ {
		counts[localID%batches]++
	}
	for batch, count := range counts {
		if count > 2 {
			t.Fatalf("batch %d contains %d sources, parallel target is 2", batch, count)
		}
	}
}

func TestBetweennessBatchPlanFitsStepBudget(t *testing.T) {
	batches, err := planBetweennessBatches(1_000_000, 1, 10)
	if err != nil {
		t.Fatalf("default-step batch plan failed: %v", err)
	}
	if batches != 9 {
		t.Fatalf("default max_step=10 must cap source batches at 9, got %d", batches)
	}
	if 1+batches > 10 {
		t.Fatalf("adjacency plus source batches exceed max_step: %d", 1+batches)
	}

	batches, err = planBetweennessBatches(9, 2, 3)
	if err != nil {
		t.Fatalf("short-step batch plan failed: %v", err)
	}
	if batches != 2 {
		t.Fatalf("max_step=3 must use two source batches, got %d", batches)
	}
	if _, err := planBetweennessBatches(1, 1, 1); err == nil {
		t.Fatal("max_step=1 must be rejected")
	}
}

func TestBetweennessInactiveBatchPayloadIsConstant(t *testing.T) {
	var encoded buffer.EncodeBuffer
	encoded.Init(1024)
	offset := serialize.SUint32(100)
	if err := encoded.Marshal(&offset); err != nil {
		t.Fatalf("marshal offset: %v", err)
	}
	for i := 0; i < 10000; i++ {
		if err := encoded.Marshal(&noBetweennessVertexValue); err != nil {
			t.Fatalf("marshal empty value %d: %v", i, err)
		}
	}
	if got := len(encoded.PayLoad()); got != 4 {
		t.Fatalf("10000 inactive vertices produced %d payload bytes, expected 4", got)
	}
}

func TestBetweennessSourceSamplingIsDeterministic(t *testing.T) {
	for vertexID := uint32(0); vertexID < 100; vertexID++ {
		first := sampleBetweennessSource(vertexID, 0.25)
		second := sampleBetweennessSource(vertexID, 0.25)
		if first != second {
			t.Fatalf("source %d sampling is not deterministic", vertexID)
		}
		if !sampleBetweennessSource(vertexID, 1) {
			t.Fatalf("exact mode skipped source %d", vertexID)
		}
	}
}

func TestSumBetweennessPartials(t *testing.T) {
	result, err := sumBetweennessPartials([]serialize.SliceFloat32{
		{1, 2, 3},
		{0.5, 1, 1.5},
	}, 3)
	if err != nil {
		t.Fatalf("sum partials: %v", err)
	}
	expected := []float64{1.5, 3, 4.5}
	for vertexID, value := range result {
		if math.Abs(float64(value)-expected[vertexID]) > 1e-6 {
			t.Fatalf("vertex %d expected %v, got %v",
				vertexID, expected[vertexID], value)
		}
	}
	if _, err := sumBetweennessPartials([]serialize.SliceFloat32{{1}}, 2); err == nil {
		t.Fatal("mismatched partial length was accepted")
	}
}

func TestBetweennessOutputRequiresFinalGlobalVector(t *testing.T) {
	worker := &BetweennessCentralityWorker{totalVertices: 3}
	context := worker.MakeContext(nil)
	context.CreateValue(betweennessFinalValue, compute.ValueTypeSliceFloat32,
		compute.CValueActionNormal)
	context.SetValue(betweennessFinalValue, serialize.SliceFloat32{})
	if err := worker.ValidateOutput(); err == nil {
		t.Fatal("expected missing global result to reject output")
	}

	expected := serialize.SliceFloat32{0, 1, 0}
	context.SetValue(betweennessFinalValue, expected)
	if err := worker.ValidateOutput(); err != nil {
		t.Fatalf("completed global result rejected: %v", err)
	}
	if len(worker.betweennessValues) != len(expected) ||
		worker.betweennessValues[1] != expected[1] {
		t.Fatalf("global result was not installed: %v", worker.betweennessValues)
	}
}
