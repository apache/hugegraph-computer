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
	"strings"
	"sync/atomic"
	"testing"
)

func TestLouvainDefensivelyRejectsUnpublishedCommunityIDs(t *testing.T) {
	// The normal master/worker barrier completes BeforeStep before output starts.
	// This test covers the defensive guard against malformed or early output state.
	louvain := &LouvainWorker{firstStep: false}
	weighted := &LouvainWeightedWorker{firstStep: false}
	tests := []struct {
		name     string
		validate func() error
		publish  func()
	}{
		{"louvain", louvain.ValidateOutput, func() { atomic.StoreInt32(&louvain.outputReady, 1) }},
		{"louvain_weighted", weighted.ValidateOutput, func() { atomic.StoreInt32(&weighted.outputReady, 1) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.validate()
			if err == nil || !strings.Contains(err.Error(), "increase compute.max_step") {
				t.Fatalf("expected actionable unpublished-output error, got %v", err)
			}
			test.publish()
			if err := test.validate(); err != nil {
				t.Fatalf("published output rejected: %v", err)
			}
		})
	}
}
