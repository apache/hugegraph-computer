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
	"reflect"
	"strconv"
	"testing"
	"vermeer/apps/common"
	"vermeer/apps/options"
	"vermeer/apps/serialize"
	"vermeer/apps/structure"
)

func init() {
	options.Init()
}

func TestSortedUndirectedNeighborsDeduplicatesAndDropsSelfLoops(t *testing.T) {
	got := sortedUndirectedNeighbors(5, 8,
		serialize.SliceUint32{7, 3, 5, 7},
		serialize.SliceUint32{2, 3, 5})
	want := serialize.SliceUint32{2, 3, 7}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestSortedUndirectedNeighborsDropsInvalidRemoteIDs(t *testing.T) {
	got := sortedUndirectedNeighbors(0, 3,
		serialize.SliceUint32{1, 99},
		serialize.SliceUint32{2, 99})
	want := serialize.SliceUint32{1, 2}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected %v after invalid-ID filtering, got %v", want, got)
	}
}

func TestDegreeOrientation(t *testing.T) {
	degrees := []serialize.SUint32{3, 1, 3, 5}
	neighbors := serialize.SliceUint32{0, 1, 2, 3}
	got := orientedNeighbors(2, neighbors, degrees)
	want := serialize.SliceUint32{3}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	if cap(got) != len(got) {
		t.Fatalf("oriented neighbors retain capacity %d for %d values", cap(got), len(got))
	}
	if !rankBefore(0, 2, degrees) || rankBefore(2, 0, degrees) {
		t.Fatal("equal-degree IDs did not provide a stable total order")
	}
}

func TestSortedIntersectionCount(t *testing.T) {
	tests := []struct {
		name  string
		left  serialize.SliceUint32
		right serialize.SliceUint32
		want  uint64
	}{
		{
			name:  "linear_merge",
			left:  serialize.SliceUint32{1, 3, 4, 8},
			right: serialize.SliceUint32{0, 3, 8, 9},
			want:  2,
		},
		{
			name: "binary_search",
			left: serialize.SliceUint32{5, 99},
			right: serialize.SliceUint32{
				0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
				10, 11, 12, 13, 14, 15, 16, 17, 99,
			},
			want: 2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := sortedIntersectionCount(test.left, test.right); got != test.want {
				t.Fatalf("expected %d, got %d", test.want, got)
			}
		})
	}
}

func TestTriangleCountAndClusteringOnK4(t *testing.T) {
	graph := completeUndirectedGraph(4)

	triangle := newTriangleTestWorker(t, graph, 3)
	runThreeStepTriangleWorker(triangle, 4)
	for vertexID, value := range triangle.triangleCount {
		if value != 3 {
			t.Fatalf("vertex %d expected 3 triangles, got %d", vertexID, value)
		}
	}

	coefficient := newClusteringTestWorker(t, graph, 3)
	runThreeStepClusteringWorker(coefficient, 4)
	for vertexID, value := range coefficient.ccValues {
		if math.Abs(float64(value)-1) > 1e-6 {
			t.Fatalf("vertex %d expected coefficient 1, got %v", vertexID, value)
		}
	}
}

func TestTriangleCountAndClusteringDoNotCountOpenWedge(t *testing.T) {
	graph := &structure.GraphData{
		VertexCount: 3,
		Vertex: &structure.VertexMem{
			TotalVertex: make([]structure.Vertex, 3),
		},
		Edges: &structure.EdgeMem{
			InEdges: serialize.TwoDimSliceUint32{
				{1}, {0, 2}, {1},
			},
			OutEdges: serialize.TwoDimSliceUint32{
				{1}, {0, 2}, {1},
			},
		},
	}

	triangle := newTriangleTestWorker(t, graph, 3)
	runThreeStepTriangleWorker(triangle, 3)
	for vertexID, value := range triangle.triangleCount {
		if value != 0 {
			t.Fatalf("open-wedge vertex %d reported %d triangles", vertexID, value)
		}
	}

	coefficient := newClusteringTestWorker(t, graph, 3)
	runThreeStepClusteringWorker(coefficient, 3)
	for vertexID, value := range coefficient.ccValues {
		if value != 0 {
			t.Fatalf("open-wedge vertex %d reported coefficient %v", vertexID, value)
		}
	}
}

func TestTriangleWorkersTreatAsymmetricInputAsUndirected(t *testing.T) {
	graph := &structure.GraphData{
		VertexCount: 3,
		Vertex: &structure.VertexMem{
			TotalVertex: make([]structure.Vertex, 3),
		},
		Edges: &structure.EdgeMem{
			// Directed input 0 -> 1 -> 2 -> 0. Unioning in/out adjacency
			// produces one undirected triangle, matching the existing contract.
			InEdges: serialize.TwoDimSliceUint32{
				{2}, {0}, {1},
			},
			OutEdges: serialize.TwoDimSliceUint32{
				{1}, {2}, {0},
			},
		},
	}

	worker := newTriangleTestWorker(t, graph, 3)
	runThreeStepTriangleWorker(worker, 3)
	for vertexID, value := range worker.triangleCount {
		if value != 1 {
			t.Fatalf("vertex %d expected one triangle, got %d", vertexID, value)
		}
	}
}

func TestTriangleCountUsesInt64Output(t *testing.T) {
	worker := &TriangleCountWorker{
		triangleCount: []serialize.SInt64{serialize.SInt64(math.MaxInt32) + 1},
	}
	context := worker.MakeContext(nil)
	context.Output = true
	value, ok := worker.VertexValue(0).(*serialize.SInt64)
	if !ok {
		t.Fatalf("triangle output type is %T, expected *serialize.SInt64",
			worker.VertexValue(0))
	}
	if int64(*value) != int64(math.MaxInt32)+1 {
		t.Fatalf("64-bit triangle count was truncated: %d", *value)
	}
	if worker.OutputValueType() != common.HgValueTypeLong {
		t.Fatalf("triangle HugeGraph output type is %q, expected LONG",
			worker.OutputValueType())
	}
}

func TestTriangleStepThreeScatterPayloadIsEmpty(t *testing.T) {
	worker := &TriangleCountWorker{}
	context := worker.MakeContext(nil)
	context.Step = 3
	value := worker.VertexValue(0)
	if size := value.PredictSize(); size != 0 {
		t.Fatalf("step-three scatter value predicts %d bytes, expected 0", size)
	}
	if _, err := value.Unmarshal([]byte{1}); err == nil {
		t.Fatal("non-empty step-three scatter payload was accepted")
	}
}

func TestTriangleAlgorithmsRejectShortStepBudget(t *testing.T) {
	if err := validateTriangleMaxStep(2); err == nil {
		t.Fatal("compute.max_step=2 was accepted")
	}
	if err := validateTriangleMaxStep(3); err != nil {
		t.Fatalf("compute.max_step=3 was rejected: %v", err)
	}
}

func completeUndirectedGraph(vertexCount uint32) *structure.GraphData {
	inEdges := make(serialize.TwoDimSliceUint32, vertexCount)
	outEdges := make(serialize.TwoDimSliceUint32, vertexCount)
	for vertexID := uint32(0); vertexID < vertexCount; vertexID++ {
		for neighborID := uint32(0); neighborID < vertexCount; neighborID++ {
			if vertexID == neighborID {
				continue
			}
			inEdges[vertexID] = append(inEdges[vertexID],
				serialize.SUint32(neighborID))
			outEdges[vertexID] = append(outEdges[vertexID],
				serialize.SUint32(neighborID))
		}
	}
	return &structure.GraphData{
		VertexCount: vertexCount,
		Vertex: &structure.VertexMem{
			TotalVertex: make([]structure.Vertex, vertexCount),
		},
		Edges: &structure.EdgeMem{
			InEdges:  inEdges,
			OutEdges: outEdges,
		},
	}
}

func newTriangleTestWorker(t *testing.T, graph *structure.GraphData,
	maxStep int) *TriangleCountWorker {
	t.Helper()
	worker := &TriangleCountWorker{}
	context := worker.MakeContext(map[string]string{
		"compute.max_step": strconv.Itoa(maxStep),
	})
	context.GraphData = graph
	context.Parallel = 1
	if err := worker.Init(); err != nil {
		t.Fatalf("triangle init: %v", err)
	}
	return worker
}

func newClusteringTestWorker(t *testing.T, graph *structure.GraphData,
	maxStep int) *ClusteringCoefficientWorker {
	t.Helper()
	worker := &ClusteringCoefficientWorker{}
	context := worker.MakeContext(map[string]string{
		"compute.max_step": strconv.Itoa(maxStep),
	})
	context.GraphData = graph
	context.Parallel = 1
	if err := worker.Init(); err != nil {
		t.Fatalf("clustering init: %v", err)
	}
	return worker
}

func runThreeStepTriangleWorker(worker *TriangleCountWorker, vertexCount uint32) {
	for step := int32(1); step <= triangleComputeSteps; step++ {
		worker.WContext.Step = step
		for vertexID := uint32(0); vertexID < vertexCount; vertexID++ {
			worker.Compute(vertexID, 0)
		}
	}
}

func runThreeStepClusteringWorker(worker *ClusteringCoefficientWorker,
	vertexCount uint32) {
	for step := int32(1); step <= triangleComputeSteps; step++ {
		worker.WContext.Step = step
		for vertexID := uint32(0); vertexID < vertexCount; vertexID++ {
			worker.Compute(vertexID, 0)
		}
	}
}
