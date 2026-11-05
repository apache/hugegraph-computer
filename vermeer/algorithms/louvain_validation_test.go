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
	"strings"
	"testing"
	"vermeer/apps/compute"
	"vermeer/apps/options"
	"vermeer/apps/serialize"
	"vermeer/apps/structure"
)

func init() {
	options.Init()
}

func TestIdentityCommunityIDs(t *testing.T) {
	ids := identityCommunityIDs(4)
	assertIdentityCommunityIDs(t, ids, 4)
}

func TestLouvainFloatParameterValidation(t *testing.T) {
	invalidPositive := []float64{0, -1, math.NaN(), math.Inf(1), math.Inf(-1)}
	for _, value := range invalidPositive {
		if err := validatePositiveFinite("value", value); err == nil {
			t.Fatalf("expected positive validation to reject %v", value)
		}
	}
	if err := validatePositiveFinite("value", 1); err != nil {
		t.Fatalf("valid positive value rejected: %v", err)
	}

	invalidNonNegative := []float64{-1, math.NaN(), math.Inf(1), math.Inf(-1)}
	for _, value := range invalidNonNegative {
		if err := validateNonNegativeFinite("value", value); err == nil {
			t.Fatalf("expected non-negative validation to reject %v", value)
		}
	}
	for _, value := range []float64{0, 1} {
		if err := validateNonNegativeFinite("value", value); err != nil {
			t.Fatalf("valid non-negative value %v rejected: %v", value, err)
		}
	}
}

func TestLouvainStepValidation(t *testing.T) {
	for _, value := range []int{0, -1} {
		if err := validatePositiveInt("louvain.step", value); err == nil {
			t.Fatalf("expected step validation to reject %d", value)
		}
	}
	if err := validatePositiveInt("louvain.step", 1); err != nil {
		t.Fatalf("valid step rejected: %v", err)
	}
}

func TestLouvainZeroEffectiveEdgesPublishIdentityOutput(t *testing.T) {
	t.Run("unweighted", func(t *testing.T) {
		worker := &LouvainWorker{}
		ctx := worker.MakeContext(map[string]string{"louvain.resolution": "1"})
		ctx.GraphData = newLouvainTestGraph(3)
		ctx.Parallel = 1
		ctx.Step = 2

		if err := worker.Init(); err != nil {
			t.Fatalf("init failed: %v", err)
		}
		worker.BeforeStep()

		if err := worker.ValidateOutput(); err != nil {
			t.Fatalf("zero-edge result was not published: %v", err)
		}
		assertIdentityCommunityIDs(t, worker.nodeID, 3)
		if worker.firstStep {
			t.Fatal("zero-edge early return left firstStep set")
		}
		if worker.neighborEdges != nil {
			t.Fatal("zero-edge early return retained adjacency state")
		}
		if got := ctx.GetValue("mod_value").(serialize.SFloat32); got != 0 {
			t.Fatalf("expected zero modularity, got %v", got)
		}
	})

	t.Run("weighted", func(t *testing.T) {
		worker := &LouvainWeightedWorker{}
		ctx := worker.MakeContext(map[string]string{"louvain.resolution": "1"})
		ctx.GraphData = newLouvainTestGraph(3)
		ctx.Parallel = 1
		ctx.Step = 3

		if err := worker.Init(); err != nil {
			t.Fatalf("init failed: %v", err)
		}
		worker.BeforeStep()

		if err := worker.ValidateOutput(); err != nil {
			t.Fatalf("zero-weight result was not published: %v", err)
		}
		assertIdentityCommunityIDs(t, worker.nodeID, 3)
		if worker.firstStep {
			t.Fatal("zero-weight early return left firstStep set")
		}
		if worker.neighborEdges != nil {
			t.Fatal("zero-weight early return retained adjacency state")
		}
		if got := ctx.GetValue("mod_value").(serialize.SFloat32); got != 0 {
			t.Fatalf("expected zero modularity, got %v", got)
		}
	})
}

func TestWeightedLouvainInitRejectsInvalidEdgeWeights(t *testing.T) {
	tests := []struct {
		name   string
		weight serialize.SFloat32
	}{
		{"negative", serialize.SFloat32(-1)},
		{"nan", serialize.SFloat32(float32(math.NaN()))},
		{"positive_inf", serialize.SFloat32(float32(math.Inf(1)))},
		{"negative_inf", serialize.SFloat32(float32(math.Inf(-1)))},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			graph := newLouvainTestGraph(1)
			graph.VertIDStart = 7
			graph.Edges = &structure.EdgeMem{
				InEdges: serialize.TwoDimSliceUint32{{0}},
			}
			properties := structure.EdgeProperties{
				"weight": &structure.VValues{
					VType:  structure.ValueTypeFloat32,
					Values: [][]serialize.SFloat32{{test.weight}},
				},
			}
			graph.InEdgesProperty = &properties

			worker := &LouvainWeightedWorker{}
			ctx := worker.MakeContext(map[string]string{
				"louvain.resolution":           "1",
				"louvain.edge_weight_property": "weight",
			})
			ctx.GraphData = graph
			ctx.Parallel = 1

			err := worker.Init()
			if err == nil {
				t.Fatalf("expected invalid edge weight %v to fail init", test.weight)
			}
			for _, expected := range []string{"weight", "vertex 7", "incoming edge 0"} {
				if !strings.Contains(err.Error(), expected) {
					t.Fatalf("error %q does not contain %q", err, expected)
				}
			}
		})
	}
}

func TestWeightedLouvainSkipsEffectivelyIsolatedVertex(t *testing.T) {
	edges := &structure.EdgeMem{
		InEdges:  serialize.TwoDimSliceUint32{{0}},
		OutEdges: serialize.TwoDimSliceUint32{{}},
	}
	worker := &LouvainWeightedWorker{
		WorkerComputerBase: compute.WorkerComputerBase{
			WContext: &compute.CWContext{
				Step: 3,
				GraphData: &structure.GraphData{
					VertIDStart: 0,
					VertexCount: 1,
					Edges:       edges,
				},
			},
		},
		firstStep:       true,
		firstStepKI:     make([]float64, 1),
		firstStepCommID: make([]serialize.SUint32, 1),
		neighborEdges:   []serialize.MapUint32Float32{make(serialize.MapUint32Float32)},
		communities:     make(map[serialize.SUint32]*community),
		node2comm:       []map[serialize.SUint32]serialize.SUint32{make(map[serialize.SUint32]serialize.SUint32)},
	}

	worker.Compute(0, 0)
	if len(worker.node2comm[0]) != 0 {
		t.Fatalf("effectively isolated vertex was scheduled for a community move: %v",
			worker.node2comm[0])
	}
}

func newLouvainTestGraph(vertexCount uint32) *structure.GraphData {
	return &structure.GraphData{
		VertexCount: vertexCount,
		Vertex: &structure.VertexMem{
			TotalVertex: make([]structure.Vertex, vertexCount),
		},
		Edges: &structure.EdgeMem{
			InEdges: make(serialize.TwoDimSliceUint32, vertexCount),
		},
	}
}

func assertIdentityCommunityIDs(t *testing.T, ids []serialize.SUint32, total uint32) {
	t.Helper()
	if uint32(len(ids)) != total {
		t.Fatalf("expected %d IDs, got %d", total, len(ids))
	}
	for i, id := range ids {
		if uint32(id) != uint32(i) {
			t.Fatalf("vertex %d assigned to community %d", i, id)
		}
	}
}
