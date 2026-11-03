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

package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
	"vermeer/apps/common"
	"vermeer/apps/compute"
	"vermeer/apps/options"
	pb "vermeer/apps/protos"
	"vermeer/apps/structure"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
)

type invalidOutputWorker struct {
	compute.WorkerComputerBase
	validated bool
}

func (w *invalidOutputWorker) ValidateOutput() error {
	w.validated = true
	return errors.New("increase compute.max_step")
}

type outputStatusClient struct {
	pb.MasterClient
	req *pb.ComputeTaskStatusReq
}

func (c *outputStatusClient) ComputeTaskStatus(_ context.Context, req *pb.ComputeTaskStatusReq,
	_ ...grpc.CallOption) (*pb.ComputeTaskStatusResp, error) {
	c.req = req
	return &pb.ComputeTaskStatusResp{}, nil
}

func TestRunOutputValidatesBeforeWriting(t *testing.T) {
	previousOptions := options.AllOptionsMap
	previousRunning := common.PrometheusMetrics.TaskRunningCnt
	previousMaster := ServiceWorker.MasterClient
	options.Init()
	ComputeTaskMgr.Init()
	common.PrometheusMetrics.TaskRunningCnt = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "test_output_validation_running"}, []string{"type"})
	t.Cleanup(func() {
		ComputeTaskMgr.Init()
		options.AllOptionsMap = previousOptions
		common.PrometheusMetrics.TaskRunningCnt = previousRunning
		ServiceWorker.MasterClient = previousMaster
	})
	for _, tc := range []struct {
		name   string
		action int32
		state  structure.TaskState
	}{
		{"invalid", structure.ActionDoNoting, structure.TaskStateError},
		{"cancelled", structure.ActionCancelTask, structure.TaskStateCanceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status := new(outputStatusClient)
			ServiceWorker.MasterClient = status
			task := &structure.TaskInfo{ID: 9300 + int32(tc.action), Type: structure.TaskTypeCompute,
				Action: tc.action, Params: map[string]string{"compute.algorithm": "louvain"}}
			worker := new(invalidOutputWorker)
			ct := ComputeTaskMgr.MakeTask(task)
			ct.ComputeWorker = worker
			new(ComputeBl).RunOutput(task.ID)
			if task.State != tc.state {
				t.Fatalf("state=%s, want %s", task.State, tc.state)
			}
			if tc.action == structure.ActionCancelTask {
				if worker.validated || status.req != nil {
					t.Fatal("cancelled task must not validate or report an output error")
				}
			} else if !worker.validated || status.req == nil || status.req.ErrorMsg != "increase compute.max_step" {
				t.Fatalf("expected validation error reported to Master, got %+v", status.req)
			}
		})
	}
	t.Run("paused_then_cancelled", func(t *testing.T) {
		status := new(outputStatusClient)
		ServiceWorker.MasterClient = status
		task := &structure.TaskInfo{ID: 9340, Type: structure.TaskTypeCompute,
			Action: structure.ActionPauseTask, Params: map[string]string{"compute.algorithm": "louvain"}}
		worker := new(invalidOutputWorker)
		ct := ComputeTaskMgr.MakeTask(task)
		ct.ComputeWorker = worker
		done := make(chan struct{})
		go func() {
			new(ComputeBl).RunOutput(task.ID)
			close(done)
		}()
		time.Sleep(50 * time.Millisecond)
		atomic.StoreInt32(&task.Action, structure.ActionCancelTask)
		select {
		case <-done:
		case <-time.After(12 * time.Second):
			t.Fatal("paused output did not observe cancellation")
		}
		if task.State != structure.TaskStateCanceled || worker.validated || status.req != nil {
			t.Fatalf("paused cancellation must not report output error: state=%s request=%+v", task.State, status.req)
		}
	})
}
