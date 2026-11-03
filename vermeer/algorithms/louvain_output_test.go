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
	"testing"
)

func TestLouvainRejectsOutputBeforeFirstPass(t *testing.T) {
	for _, test := range []struct {
		name   string
		worker interface{ ValidateOutput() error }
	}{
		{"louvain", &LouvainWorker{firstStep: true}},
		{"louvain_weighted", &LouvainWeightedWorker{firstStep: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.worker.ValidateOutput()
			if err == nil || !strings.Contains(err.Error(), "increase compute.max_step") {
				t.Fatalf("expected actionable early-output error, got %v", err)
			}
		})
	}
	if err := (&LouvainWorker{firstStep: false}).ValidateOutput(); err != nil {
		t.Fatalf("completed pass rejected: %v", err)
	}
	if err := (&LouvainWeightedWorker{firstStep: false}).ValidateOutput(); err != nil {
		t.Fatalf("completed weighted pass rejected: %v", err)
	}
}
