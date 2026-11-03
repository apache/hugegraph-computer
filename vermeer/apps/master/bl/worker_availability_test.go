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

package bl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
	"vermeer/apps/common"
	"vermeer/apps/master/schedules"
	"vermeer/apps/master/workers"
	pb "vermeer/apps/protos"
	"vermeer/apps/structure"
	"vermeer/apps/version"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

type registrationWindowWorker struct {
	pb.UnimplementedWorkerServer
	started chan struct{}
	release chan struct{}
}

func TestRegistrationVersionMismatchReportsCorrectVersions(t *testing.T) {
	workerVersion := "incompatible-worker-version"
	resp, err := new(ServerHandler).SayHelloMaster(context.Background(), &pb.HelloMasterReq{
		Version: workerVersion,
	})
	want := fmt.Sprintf("registration denied: inconsistent registration version! master version %s, worker version %s", version.Version, workerVersion)
	if err == nil || err.Error() != want || resp.GetBase().GetMessage() != want {
		t.Fatalf("unexpected response for version mismatch: response=%v error=%v", resp, err)
	}
}

type dispatchTestStream struct {
	grpc.ServerStream
	requests []*pb.LoadGraphTaskResp
	sendErr  error
	stopErr  error
}

func (s *dispatchTestStream) Send(req *pb.LoadGraphTaskResp) error {
	s.requests = append(s.requests, req)
	if req.Step == pb.LoadStep_Error {
		return s.stopErr
	}
	return s.sendErr
}

func (s *dispatchTestStream) Recv() (*pb.LoadGraphTaskReq, error) {
	return nil, io.EOF
}

type serializedLoadTestStream struct {
	dispatchTestStream
	entered chan struct{}
	release chan struct{}
	second  chan struct{}
	active  atomic.Int32
	peak    atomic.Int32
	calls   atomic.Int32
}

type publicationTestStream struct {
	grpc.ServerStream
	ctx    context.Context
	onRead func(int32)
	reads  atomic.Int32
}

func (s *publicationTestStream) Context() context.Context {
	if s.onRead != nil {
		s.onRead(s.reads.Add(1))
	}
	return s.ctx
}

func (s *publicationTestStream) Send(*pb.SuperStepResp) error { return nil }

// This checks registration ordering with controlled handlers, not a live reconnect.
func testSuperStepPublication(t *testing.T, worker *workers.WorkerClient) {
	ServerMgr.Init()
	workerMgr.SetTaskStream(worker, workers.LoadTaskStream, context.Background())
	workerMgr.SetTaskStream(worker, workers.ComputeTaskStream, context.Background())
	oldCtx, cancelOld := context.WithCancel(context.Background())
	newCtx, cancelNew := context.WithCancel(context.Background())
	defer cancelOld()
	defer cancelNew()
	paused, release, published := make(chan struct{}), make(chan struct{}), make(chan struct{})
	oldStream := &publicationTestStream{
		ctx: metadata.NewIncomingContext(oldCtx, metadata.Pairs("worker_name", worker.Name)),
		onRead: func(read int32) {
			if read == 2 {
				close(paused)
				<-release
			}
		},
	}
	newStream := &publicationTestStream{
		ctx: metadata.NewIncomingContext(newCtx, metadata.Pairs("worker_name", worker.Name)),
		onRead: func(read int32) {
			if read == 3 {
				close(published)
			}
		},
	}
	oldDone, newDone := make(chan error, 1), make(chan error, 1)
	go func() { oldDone <- new(ServerHandler).SuperStep(&pb.SuperStepReq{}, oldStream) }()
	<-paused
	started := make(chan struct{})
	go func() {
		close(started)
		newDone <- new(ServerHandler).SuperStep(&pb.SuperStepReq{}, newStream)
	}()
	<-started
	select {
	case <-published:
		t.Error("replacement registration overtook an unpublished dispatch entry")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-published
	cancelOld()
	if err := <-oldDone; err != nil {
		t.Error(err)
	}
	if !workerMgr.IsWorkerReady(worker) || ServerMgr.SuperStepServer(worker.Name).streamServer != newStream {
		t.Error("old publication or cleanup replaced the live stream's readiness")
	}
	cancelNew()
	if err := <-newDone; err != nil {
		t.Error(err)
	}
	if workerMgr.IsWorkerReady(worker) {
		t.Error("a canceled replacement must no longer be ready")
	}
}

func (s *serializedLoadTestStream) Send(*pb.LoadGraphTaskResp) error {
	active := s.active.Add(1)
	defer s.active.Add(-1)
	for peak := s.peak.Load(); active > peak; peak = s.peak.Load() {
		if s.peak.CompareAndSwap(peak, active) {
			break
		}
	}
	if s.calls.Add(1) == 1 {
		close(s.entered)
		<-s.release
		return s.sendErr
	}
	close(s.second)
	return nil
}

func TestWorkerAvailabilitySerializesLoadSends(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(fmt.Sprintf("first_send_fails_%v", failed), func(t *testing.T) {
			stream := &serializedLoadTestStream{
				entered: make(chan struct{}), release: make(chan struct{}), second: make(chan struct{}),
			}
			if failed {
				stream.sendErr = errors.New("fixture send failure")
			}
			server := &LoadGraphServer{streamServer: stream}
			first, second := make(chan error, 1), make(chan error, 1)
			go func() { first <- server.SendLoadReq(&LoadingTaskReq{TaskId: 1, Step: pb.LoadStep_Vertex}) }()
			<-stream.entered
			started := make(chan struct{})
			go func() {
				close(started)
				second <- server.AsyncLoad(1, pb.LoadStep_Error, "", "graph", "space", nil, nil)
			}()
			<-started
			select {
			case <-stream.second:
				t.Error("a second underlying Send entered before the first completed")
			case <-time.After(50 * time.Millisecond):
			}
			close(stream.release)
			if err := <-first; !errors.Is(err, stream.sendErr) {
				t.Fatalf("original send error changed: %v", err)
			}
			if err := <-second; err != nil {
				t.Fatal(err)
			}
			if stream.peak.Load() != 1 || stream.calls.Load() != 2 {
				t.Fatal("all load sends must be serialized and the lock released after errors")
			}
		})
	}
}

func (w *registrationWindowWorker) RuntimeAction(context.Context, *pb.RuntimeActionReq) (*pb.RuntimeActionResp, error) {
	close(w.started)
	<-w.release
	return &pb.RuntimeActionResp{Response: &pb.RuntimeActionResp_MemoryLimitResp{
		MemoryLimitResp: &pb.SetMemoryLimitResp{},
	}}, nil
}

// This executes real SayHelloMaster while its ordinary memory-limit RPC is pending.
// No worker task stream can exist yet: the worker only opens those after hello returns.
func TestWorkerAvailabilityWaitsForRegistration(t *testing.T) {
	previousWorkers := common.PrometheusMetrics.WorkerCnt
	previousTasks := common.PrometheusMetrics.TaskCnt
	previousRunning := common.PrometheusMetrics.TaskRunningCnt
	t.Cleanup(func() {
		common.PrometheusMetrics.WorkerCnt = previousWorkers
		common.PrometheusMetrics.TaskCnt = previousTasks
		common.PrometheusMetrics.TaskRunningCnt = previousRunning
	})
	common.PrometheusMetrics.WorkerCnt = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "test_registration_workers"}, nil)
	common.PrometheusMetrics.TaskCnt = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "test_registration_tasks"}, []string{"type"})
	common.PrometheusMetrics.TaskRunningCnt = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "test_registration_running"}, []string{"type"})
	workers.WorkerManager.Init()
	structure.SpaceManager.Init()
	graphMgr.Init()
	taskMgr.Init()
	if err := taskMgr.InitStore(); err != nil {
		t.Fatal(err)
	}
	ServerMgr.Init()
	Scheduler.resourceManager = &schedules.SchedulerResourceManager{}
	Scheduler.resourceManager.Init()
	Scheduler.taskManager = new(schedules.SchedulerTaskManager).Init()
	Scheduler.algorithmManager = &schedules.SchedulerAlgorithmManager{}
	Scheduler.algorithmManager.Init()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	w := &registrationWindowWorker{started: make(chan struct{}), release: make(chan struct{})}
	server := grpc.NewServer()
	pb.RegisterWorkerServer(server, w)
	go server.Serve(listener)
	defer server.Stop()
	returned := make(chan error, 1)
	ctx := peer.NewContext(context.Background(), &peer.Peer{Addr: listener.Addr()})
	go func() {
		_, err := new(ServerHandler).SayHelloMaster(ctx, &pb.HelloMasterReq{
			WorkerPeer: listener.Addr().String(), Version: version.Version,
		})
		returned <- err
	}()
	select {
	case <-w.started:
	case <-time.After(3 * time.Second):
		t.Fatal("registration did not reach memory RPC")
	}
	defer func() {
		close(w.release)
		select {
		case err := <-returned:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(time.Second):
			t.Error("hello did not return")
		}
	}()
	if workers.WorkerManager.HasAvailableWorkers("current", nil) {
		t.Fatal("hello still pending: reverse dial must not advertise task readiness")
	}
	task, err := taskMgr.CreateTask("current", structure.TaskTypeLoad, 0)
	if err != nil {
		t.Fatal(err)
	}
	task.GraphName = "graph"
	task.CreateType = structure.TaskCreateSync
	if _, err := taskMgr.AddTask(task); err != nil {
		t.Fatal(err)
	}
	agent, status, _, err := new(schedules.Broker).Init().ApplyAgent(task)
	if err != nil || agent != nil || status != schedules.AgentStatusWorkerNotReady {
		t.Fatalf("scheduler admitted a worker before its task streams: %v %v", status, err)
	}
	registered := workers.WorkerManager.GetAllWorkers()
	if len(registered) != 1 {
		t.Fatal("wrong registration count")
	}
	worker := registered[0]
	t.Cleanup(worker.Close)
	t.Run("superstep_publication_order", func(t *testing.T) { testSuperStepPublication(t, worker) })
	if ServerMgr.LoadServer(worker.Name) != nil {
		t.Fatal("unexpected load stream")
	}
	err = sendLoadingReq(task, registered, func(*workers.WorkerClient) *LoadingTaskReq {
		return &LoadingTaskReq{}
	})
	if err == nil {
		t.Fatal("missing task transport must return an error")
	}
	if task.State != structure.TaskStateError || task.ErrMessage != err.Error() {
		t.Fatalf("failed synchronous dispatch left state=%s, message=%q", task.State, task.ErrMessage)
	}
	persisted, readErr := taskMgr.ReadTask(task.ID)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if persisted.State != structure.TaskStateError || persisted.ErrMessage != task.ErrMessage {
		t.Fatalf("persisted failed task has state=%s, message=%q", persisted.State, persisted.ErrMessage)
	}

	// Reuse the isolated task store to check failures after earlier workers accepted a load.
	for _, test := range []struct {
		name       string
		missing    bool
		failedSend bool
		failedStop bool
	}{
		{name: "dispatch_missing_stream", missing: true},
		{name: "dispatch_send_failure", failedSend: true},
		{name: "dispatch_cleanup_failure", missing: true, failedStop: true},
		{name: "dispatch_success"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ServerMgr.Init()
			defer ServerMgr.Init()
			first, second, third := &dispatchTestStream{}, &dispatchTestStream{}, &dispatchTestStream{}
			dispatchErr := errors.New("dispatch failed")
			if test.failedSend {
				third.sendErr = dispatchErr
			}
			if test.failedStop {
				first.stopErr = errors.New("cleanup failed")
			}
			ServerMgr.loadSevers["first"] = &LoadGraphServer{streamServer: first}
			ServerMgr.loadSevers["second"] = &LoadGraphServer{streamServer: second}
			if !test.missing {
				ServerMgr.loadSevers["third"] = &LoadGraphServer{streamServer: third}
			}
			task, err := taskMgr.CreateTask("dispatch-space", structure.TaskTypeLoad, 0)
			if err != nil {
				t.Fatal(err)
			}
			task.GraphName = "dispatch-graph"
			if _, err := taskMgr.AddTask(task); err != nil {
				t.Fatal(err)
			}
			err = sendLoadingReq(task, []*workers.WorkerClient{
				{Name: "first"}, {Name: "second"}, {Name: "third"},
			}, func(*workers.WorkerClient) *LoadingTaskReq {
				return &LoadingTaskReq{TaskId: task.ID, Step: pb.LoadStep_Vertex,
					SpaceName: task.SpaceName, GraphName: task.GraphName}
			})
			failed := test.missing || test.failedSend
			if (err != nil) != failed {
				t.Fatalf("dispatch error = %v, want failed = %v", err, failed)
			}
			if test.failedSend && !errors.Is(err, dispatchErr) {
				t.Fatalf("original dispatch error was replaced: %v", err)
			}
			if failed {
				persisted, readErr := taskMgr.ReadTask(task.ID)
				if readErr != nil {
					t.Fatal(readErr)
				}
				if task.State != structure.TaskStateError || task.ErrMessage != err.Error() ||
					persisted.State != structure.TaskStateError || persisted.ErrMessage != err.Error() {
					t.Fatal("partial dispatch failure must retain the original error in memory and storage")
				}
			}
			for _, stream := range []*dispatchTestStream{first, second} {
				want := 1
				if failed {
					want = 2
				}
				if len(stream.requests) != want || stream.requests[0].Step != pb.LoadStep_Vertex {
					t.Fatalf("successful worker received %d messages, want %d", len(stream.requests), want)
				}
				if failed {
					stop := stream.requests[1]
					if stop.Step != pb.LoadStep_Error || stop.TaskId != task.ID ||
						stop.SpaceName != task.SpaceName || stop.GraphName != task.GraphName {
						t.Fatalf("wrong cleanup scope: %v", stop)
					}
				}
			}
		})
	}

	t.Run("compute_dispatch", testPartialComputeDispatch)
}
