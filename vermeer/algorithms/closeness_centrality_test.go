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
	"sync/atomic"
	"testing"
)

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

func TestClosenessRejectsPartialOutput(t *testing.T) {
	worker := &ClosenessCentralityWorker{}
	atomic.StoreInt64(&worker.unfinished, 1)
	if err := worker.ValidateOutput(); err == nil {
		t.Fatal("expected partial output to be rejected")
	}
	atomic.StoreInt64(&worker.unfinished, 0)
	if err := worker.ValidateOutput(); err != nil {
		t.Fatalf("completed output rejected: %v", err)
	}
}
