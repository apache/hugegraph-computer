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
	"vermeer/apps/compute"
	"vermeer/apps/serialize"
	"vermeer/apps/structure"
)

func TestIdentityCommunityIDs(t *testing.T) {
	ids := identityCommunityIDs(4)
	if len(ids) != 4 {
		t.Fatalf("expected 4 IDs, got %d", len(ids))
	}
	for i, id := range ids {
		if uint32(id) != uint32(i) {
			t.Fatalf("vertex %d assigned to community %d", i, id)
		}
	}
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
