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
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"vermeer/apps/compute"
	"vermeer/apps/options"
	"vermeer/apps/serialize"
	"vermeer/apps/structure"
)

func initClosenessTestOptions(t *testing.T) {
	t.Helper()
	previous := options.AllOptionsMap
	options.Init()
	t.Cleanup(func() {
		options.AllOptionsMap = previous
	})
}

func TestClosenessDistanceMapUsesGlobalVertexID(t *testing.T) {
	distances := newClosenessDistanceMap(101, 3)
	if _, ok := distances[101]; !ok {
		t.Fatalf("global source vertex 101 is missing: %v", distances)
	}
	if _, ok := distances[0]; ok {
		t.Fatalf("local vertex ID leaked into distance map: %v", distances)
	}
}

func TestClosenessScore(t *testing.T) {
	tests := []struct {
		name      string
		distances map[uint32]int32
		total     uint32
		improved  bool
		expected  float64
	}{
		{"connected", map[uint32]int32{0: 0, 1: 1, 2: 2}, 3, true, 2.0 / 3.0},
		{"disconnected_standard", map[uint32]int32{0: 0, 1: 1}, 4, false, 1},
		{"disconnected_wf", map[uint32]int32{0: 0, 1: 1}, 4, true, 1.0 / 3.0},
		{"isolated", map[uint32]int32{0: 0}, 4, true, 0},
		{"wide_distance_sum", map[uint32]int32{0: 0, 1: math.MaxInt32, 2: math.MaxInt32}, 3, false,
			2.0 / float64(int64(math.MaxInt32)*2)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := float64(closenessScore(test.distances, test.total, test.improved))
			if math.Abs(actual-test.expected) > 1e-6*math.Max(1, math.Abs(test.expected)) {
				t.Fatalf("expected %v, got %v", test.expected, actual)
			}
		})
	}
}

func TestClosenessParameterValidation(t *testing.T) {
	for _, value := range []float64{0, -1, 1.1, math.NaN(), math.Inf(1)} {
		if err := validateProbability("sample_rate", value); err == nil {
			t.Fatalf("expected invalid probability %v to be rejected", value)
		}
	}
	for _, value := range []float64{0.1, 1} {
		if err := validateProbability("sample_rate", value); err != nil {
			t.Fatalf("valid probability %v rejected: %v", value, err)
		}
	}
	for _, value := range []int{-1, 2} {
		if _, err := parseBooleanFlag("wf_improved", value); err == nil {
			t.Fatalf("expected invalid boolean flag %d to be rejected", value)
		}
	}
}

func TestClosenessInitValidatesBeforeAllocatingGraphState(t *testing.T) {
	initClosenessTestOptions(t)
	tests := []struct {
		name   string
		params map[string]string
		want   string
	}{
		{
			name: "sample_rate",
			params: map[string]string{
				"closeness_centrality.sample_rate": "0",
				"closeness_centrality.wf_improved": "1",
			},
			want: "sample_rate",
		},
		{
			name: "wf_improved",
			params: map[string]string{
				"closeness_centrality.sample_rate": "1",
				"closeness_centrality.wf_improved": "2",
			},
			want: "wf_improved",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			worker := &ClosenessCentralityWorker{}
			ctx := worker.MakeContext(test.params)
			// Invalid parameters must be rejected before Init dereferences graph
			// storage or allocates per-vertex traversal state.
			ctx.GraphData = &structure.GraphData{}
			ctx.Parallel = 1

			err := worker.Init()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("expected %s validation error, got %v", test.want, err)
			}
		})
	}
}

func TestClosenessTraversalFinishesAtEccentricity(t *testing.T) {
	initClosenessTestOptions(t)
	const maxStep = 4
	params := map[string]string{
		"closeness_centrality.sample_rate": "1",
		"closeness_centrality.wf_improved": "1",
		"compute.max_step":                 "4",
	}

	worker := &ClosenessCentralityWorker{}
	ctx := worker.MakeContext(params)
	ctx.GraphData = newClosenessPartitionGraph()
	ctx.Parallel = 1
	if err := worker.Init(); err != nil {
		t.Fatalf("worker init failed: %v", err)
	}

	master := &ClosenessCentralityMaster{}
	masterContext := master.MakeContext(params)
	masterContext.CreateValue(closenessUnfinishedValue, compute.ValueTypeInt64,
		compute.CValueActionAggregate)

	expectedUnfinished := []serialize.SInt64{3, 2, 1, 0}
	for step := 1; step <= maxStep; step++ {
		ctx.Step = int32(step)
		worker.BeforeStep()
		for vertexID := uint32(2); vertexID < 5; vertexID++ {
			worker.Compute(vertexID, 0)
		}
		if step == 1 {
			// Step-1 scatter has made the two remote adjacency lists visible
			// before AfterStep decides whether the current frontier can grow.
			worker.neighborEdges[0] = nil
			worker.neighborEdges[1] = serialize.SliceUint32{0}
		}
		worker.AfterStep()

		gotUnfinished := ctx.GetValue(closenessUnfinishedValue).(serialize.SInt64)
		if gotUnfinished != expectedUnfinished[step-1] {
			t.Fatalf("step %d: expected %d unfinished sources, got %d",
				step, expectedUnfinished[step-1], gotUnfinished)
		}
		masterContext.SetValue(closenessUnfinishedValue, gotUnfinished)
		gotContinue := master.Compute()
		wantContinue := step < maxStep
		if gotContinue != wantContinue {
			t.Fatalf("step %d: expected continue=%v, got %v",
				step, wantContinue, gotContinue)
		}
	}

	if err := worker.ValidateOutput(); err != nil {
		t.Fatalf("complete traversal rejected at eccentricity: %v", err)
	}
	assertFloat32Close(t, worker.closenessValues[2], 1.0/3.0)
	assertFloat32Close(t, worker.closenessValues[3], 3.0/8.0)
	assertFloat32Close(t, worker.closenessValues[4], 2.0/5.0)
}

func TestClosenessOutputUsesGlobalUnfinishedCount(t *testing.T) {
	workers := []*ClosenessCentralityWorker{
		newClosenessValidationWorker(0, 1),
		newClosenessValidationWorker(1, 1),
	}
	for i, worker := range workers {
		if err := worker.ValidateOutput(); err == nil {
			t.Fatalf("worker %d accepted output while another partition was unfinished", i)
		}
	}

	for i, worker := range workers {
		worker.WContext.SetValue(closenessUnfinishedValue, serialize.SInt64(0))
		if err := worker.ValidateOutput(); err != nil {
			t.Fatalf("worker %d rejected globally complete output: %v", i, err)
		}
	}
}

func newClosenessPartitionGraph() *structure.GraphData {
	return &structure.GraphData{
		VertIDStart: 2,
		VertexCount: 3,
		Vertex: &structure.VertexMem{
			TotalVertex: make([]structure.Vertex, 5),
		},
		Edges: &structure.EdgeMem{
			// Global path: 0 -> 1 -> 2 -> 3 -> 4. This partition owns 2..4.
			InEdges: serialize.TwoDimSliceUint32{
				{1},
				{2},
				{3},
			},
		},
	}
}

func newClosenessValidationWorker(localUnfinished, globalUnfinished int64) *ClosenessCentralityWorker {
	worker := &ClosenessCentralityWorker{}
	ctx := worker.MakeContext(nil)
	ctx.CreateValue(closenessUnfinishedValue, compute.ValueTypeInt64,
		compute.CValueActionAggregate)
	ctx.SetValue(closenessUnfinishedValue, serialize.SInt64(globalUnfinished))
	atomic.StoreInt64(&worker.unfinished, localUnfinished)
	return worker
}

func assertFloat32Close(t *testing.T, actual serialize.SFloat32, expected float64) {
	t.Helper()
	if math.Abs(float64(actual)-expected) > 1e-6 {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
}
