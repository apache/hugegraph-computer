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

package worker

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
	"vermeer/apps/common"
	"vermeer/apps/graphio"
	pb "vermeer/apps/protos"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type PeerHandler struct {
	pb.UnimplementedWorkerServer
}

func (ph *PeerHandler) SayHelloPeer(ctx context.Context, req *pb.HelloPeerReq) (*pb.HelloPeerResp, error) {
	p, _ := peer.FromContext(ctx)
	logrus.Infof("peer say hello name: %s, client: %s", req.GetSourceName(), p.Addr.String())
	semi := strings.LastIndex(p.Addr.String(), ":")
	if semi < 0 {
		err := fmt.Errorf("worker grpc peer ip error: %s", req)
		logrus.Errorf(err.Error())
		return &pb.HelloPeerResp{Base: &pb.BaseResponse{ErrorCode: -1, Message: err.Error()}}, err
	}
	ip := p.Addr.String()[:semi]

	semi = strings.LastIndex(req.GetWorkerPeer(), ":")
	if semi < 0 {
		err := fmt.Errorf("worker grpc peer port error: %s", req.WorkerPeer)
		logrus.Errorf(err.Error())
		return &pb.HelloPeerResp{Base: &pb.BaseResponse{ErrorCode: -1, Message: err.Error()}}, err
	}
	port := req.GetWorkerPeer()[semi+1:]
	if ServiceWorker.WorkerName != req.TargetName {
		err := fmt.Errorf("worker grpc peer target error: %s", req.TargetName)
		logrus.Errorf(err.Error())
		return &pb.HelloPeerResp{Base: &pb.BaseResponse{ErrorCode: -2, Message: err.Error()}}, err
	}

	PeerMgr.AddPeer(req.SourceName, req.Id, ip+":"+port)
	return &pb.HelloPeerResp{}, nil
}

func incomingPeer(ctx context.Context) (string, *PeerClient, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	names := md.Get("worker_name")
	if len(names) != 1 || names[0] == "" {
		return "", nil, status.Error(codes.InvalidArgument, "worker_name metadata is required")
	}
	peer := PeerMgr.GetPeer(names[0])
	if peer == nil {
		return "", nil, status.Error(codes.FailedPrecondition, "peer must register before opening streams")
	}
	return names[0], peer, nil
}

func (ph *PeerHandler) Scatter(stream pb.Worker_ScatterServer) error {
	name, peer, err := incomingPeer(stream.Context())
	if err != nil {
		return err
	}
	return peer.ScatterHandler.receiveServer(name, stream)
}

func (ph *PeerHandler) LoadAction(stream pb.Worker_LoadActionServer) error {
	name, peer, err := incomingPeer(stream.Context())
	if err != nil {
		return err
	}
	return peer.LoadActionHandler.receiveServer(name, stream)
}

func (ph *PeerHandler) StepEnd(stream pb.Worker_StepEndServer) error {
	name, peer, err := incomingPeer(stream.Context())
	if err != nil {
		return err
	}
	return peer.StepEndHandler.receiveServer(name, stream)
}

func (ph *PeerHandler) SettingAction(stream pb.Worker_SettingActionServer) error {
	name, peer, err := incomingPeer(stream.Context())
	if err != nil {
		return err
	}
	return peer.SettingActionHandler.receiveServer(name, stream)
}

func (ph *PeerHandler) DeleteGraph(ctx context.Context, req *pb.DeleteGraphReq) (*pb.DeleteGraphResp, error) {
	_ = ctx
	if req.DeleteFile {
		GraphMgr.DeleteGraphFile(req.GetSpaceName(), req.GraphName)
	}
	GraphMgr.DeleteGraph(req.GetSpaceName(), req.GraphName)
	return &pb.DeleteGraphResp{}, nil
}

func (ph *PeerHandler) GetEdges(ctx context.Context, req *pb.GetEdgesReq) (*pb.GetEdgesResp, error) {
	_ = ctx
	eb := EdgesBl{}
	inEdges, outEdges, edgeProperty, err := eb.GetEdges(req.GetSpaceName(), req.GraphName, req.VertexId, req.Direction)
	if err != nil {
		return &pb.GetEdgesResp{}, err
	}
	return &pb.GetEdgesResp{InEdges: inEdges, OutEdges: outEdges, InEdgeProperty: edgeProperty}, nil
}

func (ph *PeerHandler) GetVertex(ctx context.Context, req *pb.GetVertexReq) (*pb.GetVertexResp, error) {
	_, _ = ctx, req

	graph := GraphMgr.GetGraphByName(req.GetSpaceName(), req.GetGraphName())
	vertexIds := req.GetVertexId()
	if graph != nil && len(vertexIds) > 0 {
		resp := &pb.GetVertexResp{}
		resp.Verts = make([]*pb.VertexInfo, 0, len(vertexIds))
		for i := 0; i < len(vertexIds); i++ {
			resp.Verts = append(resp.Verts, &pb.VertexInfo{})
			shortId, ok := graph.Data.Vertex.GetVertexIndex(vertexIds[i])
			if !ok {
				logrus.Errorf("vertexId %s not found", vertexIds[i])
				continue
			}

			if shortId < graph.Data.VertIDStart ||
				shortId >= graph.Data.VertIDStart+graph.Data.VertexCount {
				logrus.Errorf("vertexId %s not in range", vertexIds[i])
				continue
			}
			resp.Verts[i].ID = vertexIds[i]
			if graph.UseProperty {
				properties := make(map[string]string)
				for _, ps := range graph.Data.VertexPropertySchema.Schema {
					properties[ps.PropKey] = graph.Data.VertexProperty.GetValue(ps.PropKey, shortId).ToString()
				}
				resp.Verts[i].Property = properties
			}
		}
		return resp, nil
	}
	return nil, nil
}

// ControlTask 控制任务
func (ph *PeerHandler) ControlTask(ctx context.Context, req *pb.ControlTaskReq) (*pb.ControlTaskResp, error) {
	_ = ctx
	task := TaskMgr.GetTaskByID(req.GetTaskID())
	if task == nil {
		return nil, status.Error(codes.NotFound, "task is not initialized")
	}
	err := TaskMgr.SetAction(task, req.Action)
	if err != nil {
		return &pb.ControlTaskResp{}, err
	}
	if req.Action == structure.ActionCancelTask && task.Type == structure.TaskTypeCompute {
		if computeTask := ComputeTaskMgr.GetTask(task.ID); computeTask != nil {
			computeTask.Locker.Lock()
			if computeTask.WaitingForStep {
				computeTask.WaitingForStep = false
				new(ComputeBl).CheckAction(computeTask)
			}
			computeTask.Locker.Unlock()
		}
	}
	//SELECT:
	//	for {
	//		switch task.State {
	//		case structure.TaskStateLoaded, structure.TaskStateComplete:
	//			break SELECT
	//		case structure.TaskStateCanceled, structure.TaskStateError:
	//			break SELECT
	//		case structure.TaskStateCanceling:
	//			time.Sleep(100 * time.Millisecond)
	//		default:
	//			task.SetState(structure.TaskStateCanceling)
	//		}
	//	}

	return &pb.ControlTaskResp{}, nil
}

func (ph *PeerHandler) SaveGraph(ctx context.Context, req *pb.GraphPersistenceReq) (*pb.GraphPersistenceResp, error) {
	_ = ctx

	logrus.Infof("SaveGraph %v", req.GraphName)
	err := GraphMgr.SaveGraph(req.GetSpaceName(), req.GraphName, ServiceWorker.WorkerName)
	res := new(pb.GraphPersistenceResp)
	if err != nil {
		logrus.Infof("SaveGraph %v error,err=%v", req.GraphName, err.Error())
		res.Base = &pb.BaseResponse{ErrorCode: -1, Message: err.Error()}
	}
	return res, err
}

func (ph *PeerHandler) ReadGraph(ctx context.Context, req *pb.GraphPersistenceReq) (*pb.GraphPersistenceResp, error) {
	_ = ctx
	graphName := req.GetGraphName()
	res := new(pb.GraphPersistenceResp)
	logrus.Infof("ReadGraph %v", req.GraphName)
	err := GraphMgr.ReadGraph(req.GetSpaceName(), graphName, ServiceWorker.WorkerName)
	if err != nil {
		logrus.Infof("ReadGraph %v error,err=%v", req.GraphName, err.Error())
		res.Base = &pb.BaseResponse{ErrorCode: -1, Message: err.Error()}
		return res, err
	}
	return res, nil
}

func (ph *PeerHandler) WriteDisk(ctx context.Context, req *pb.GraphPersistenceReq) (*pb.GraphPersistenceResp, error) {
	_ = ctx
	graphName := req.GetGraphName()
	res := new(pb.GraphPersistenceResp)
	logrus.Infof("WriteDisk %v", req.GraphName)
	err := GraphMgr.WriteDisk(req.SpaceName, graphName, ServiceWorker.WorkerName)
	if err != nil {
		logrus.Infof("WriteDisk %v error,err=%v", req.GraphName, err.Error())
		res.Base = &pb.BaseResponse{ErrorCode: -1, Message: err.Error()}
		return res, err
	}
	return res, nil
}

func (ph *PeerHandler) GetWorkerStatInfo(ctx context.Context, req *pb.WorkerStatInfoReq) (*pb.WorkerStatInfoResp, error) {
	_ = ctx
	_ = req

	resp := &pb.WorkerStatInfoResp{Base: &pb.BaseResponse{}, MetricSchemaVersion: 1}
	mp, err := common.MachineMemUsedPercent()
	if err != nil {
		resp.Base.ErrorCode = -1
		resp.Base.Message = err.Error()
	}
	resp.MemMachineUsedPercent = mp

	pp, err := common.ProcessMemUsedPercent()
	if err != nil {
		resp.Base.ErrorCode = -1
		resp.Base.Message = err.Error()
	}
	resp.MemProcessUsedPercent = pp

	metrics, metricErr := common.ReadProcessMetrics()
	resp.MetricsObservedAt = metrics.ObservedAt
	if metricErr != nil {
		// Resource collection is independent of Worker readiness and legacy
		// memory statistics; a failed sample must not become a service error.
		resp.MetricError = metricErr.Error()
	} else {
		resp.ProcessCpuCores = metrics.CPUCores
		resp.ProcessRssBytes = metrics.RSSBytes
		resp.SampleWindowSeconds = metrics.SampleWindowSeconds
	}

	return resp, nil

}

func (ph *PeerHandler) RuntimeAction(ctx context.Context, req *pb.RuntimeActionReq) (*pb.RuntimeActionResp, error) {
	_ = ctx
	var resp *pb.RuntimeActionResp
	switch req.GetRequest().(type) {
	case *pb.RuntimeActionReq_HostInfoReq:
		hostInfo, err := RuntimeSpv.HostInfo()
		if err != nil {
			logrus.Errorf("get host info error:%v", err)
			return nil, err
		}
		resp = &pb.RuntimeActionResp{
			Response: &pb.RuntimeActionResp_HostInfoResp{
				HostInfoResp: &pb.GetHostInfoResp{
					TotalMemory:     uint32(hostInfo.TotalMemory / (1024 * 1024)),
					AvailableMemory: uint32(hostInfo.AvailableMemory / (1024 * 1024)),
					CPUCount:        uint32(hostInfo.CPUs),
				},
			},
		}
	case *pb.RuntimeActionReq_MemoryLimitReq:
		RuntimeSpv.SetMemoryLimit(
			uint64(req.GetMemoryLimitReq().MaxMemoryUsed)*1024*1024,
			uint64(req.GetMemoryLimitReq().MinRemainMemory)*1024*1024,
			req.GetMemoryLimitReq().SoftMemoryLimitRatio,
		)
		resp = &pb.RuntimeActionResp{Response: &pb.RuntimeActionResp_MemoryLimitResp{MemoryLimitResp: &pb.SetMemoryLimitResp{}}}
	case *pb.RuntimeActionReq_CPULimitReq:
		RuntimeSpv.SetMaxCPU(int(req.GetCPULimitReq().MaxCPU))
		resp = &pb.RuntimeActionResp{Response: &pb.RuntimeActionResp_CPULimitResp{CPULimitResp: &pb.SetCPULimitResp{}}}
	}

	return resp, nil
}

type LoadGraphTaskHandler struct {
	grpcStream pb.Master_LoadGraphTaskClient
}

func (rh *LoadGraphTaskHandler) HandleLoadGraphTask() {
	for {
		resp, err := rh.grpcStream.Recv()
		if err != nil {
			logrus.Errorf("recv load graph task error: %s", err)
			time.Sleep(3 * time.Second)
			ServiceWorker.ReconnectMaster()
			continue
		}
		loadBl := LoadGraphBl{}
		if resp.Step == pb.LoadStep_Vertex {
			loadBl.StartLoadGraph(resp.SpaceName, resp.TaskId, resp.GraphName, resp.Workers, resp.Params)
		} else if resp.Step == pb.LoadStep_ScatterVertex {
			loadBl.ScatterVertex(resp.TaskId)
		} else if resp.Step == pb.LoadStep_Edge {
			loadBl.LoadEdge(resp.TaskId)
		} else if resp.Step == pb.LoadStep_Complete {
			loadBl.LoadComplete(resp.TaskId)
		} else if resp.Step == pb.LoadStep_OutDegree {
			loadBl.ScatterOutDegree(resp.TaskId)
		} else if resp.Step == pb.LoadStep_Error {
			loadBl.GetStatusError(resp.TaskId)
		}
	}
}

func (rh *LoadGraphTaskHandler) LoadComplete(taskId int32, partId int32) error {
	err := rh.grpcStream.Send(&pb.LoadGraphTaskReq{
		TaskId:     taskId,
		PartId:     partId,
		WorkerName: ServiceWorker.WorkerName,
		State:      graphio.LoadPartStatusDone,
	})
	return err
}

type ComputeTaskHandler struct {
	grpcStream pb.Master_ComputeTaskClient
}

func (rh *ComputeTaskHandler) HandleComputeTask() {
	for {
		resp, err := rh.grpcStream.Recv()
		if err != nil {
			logrus.Errorf("recv compute task error: %s", err)
			time.Sleep(3 * time.Second)
			ServiceWorker.ReconnectMaster()
			continue
		}
		cb := ComputeBl{}
		sb := SettingBl{}
		switch resp.Action {
		case pb.ComputeAction_Compute:
			cb.StartCompute(resp.TaskId, resp.SpaceName, resp.GraphName, resp.Algorithm, resp.Params)
		case pb.ComputeAction_SettingOutEdges:
			sb.StartSettingOutEdges(resp.TaskId, resp.SpaceName, resp.GraphName, resp.Params)
		case pb.ComputeAction_SettingOutDegree:
			sb.StartSettingOutDegree(resp.TaskId, resp.SpaceName, resp.GraphName, resp.Params)
		}
	}
}

func (rh *ComputeTaskHandler) ComputeComplete(taskId int32) error {
	err := rh.grpcStream.Send(&pb.ComputeTaskReq{
		TaskId:     taskId,
		WorkerName: ServiceWorker.WorkerName,
	})
	return err
}

type SuperStepHandler struct {
	grpcStream pb.Master_SuperStepClient
}

func (rh *SuperStepHandler) HandleSuperStep() {
	for {
		resp, err := rh.grpcStream.Recv()
		if err != nil {
			logrus.Errorf("recv super step error: %s", err)
			time.Sleep(3 * time.Second)
			ServiceWorker.ReconnectMaster()
			continue
		}
		cb := ComputeBl{}
		if resp.Output {
			go cb.RunOutputWithValues(resp.TaskId, resp.ComputeValues)
		} else {
			go cb.RunSuperStep(resp.TaskId, resp.ComputeValues)
		}
	}
}

//func (rh *SuperStepHandler) SuperStepDone(taskId int32, status string) error {
//	err := rh.grpcStream.Send(&pb.SuperStepReq{
//		TaskId:     taskId,
//		State:     status,
//		WorkerName: ServiceWorker.WorkerName,
//	})
//	return err
//}

const (
	HandlerModeClient = byte(1)
	HandlerModeServer = byte(2)
)

type LoadActionHandler struct {
	mode       byte
	peer       *PeerClient
	generation uint64
	grpcServer pb.Worker_LoadActionServer
	grpcClient pb.Worker_LoadActionClient
	locker     sync.Mutex
}

func (dh *LoadActionHandler) SetServer(stream pb.Worker_LoadActionServer) uint64 {
	return dh.setServer(stream, nil)
}
func (dh *LoadActionHandler) setServer(stream pb.Worker_LoadActionServer, cancel context.CancelFunc) uint64 {
	dh.locker.Lock()
	defer dh.locker.Unlock()
	dh.generation = PeerMgr.bindPeerStream(dh.peer, peerLoadStream, cancel)
	dh.mode, dh.grpcServer, dh.grpcClient = HandlerModeServer, stream, nil
	return dh.generation
}
func (dh *LoadActionHandler) SetClient(stream pb.Worker_LoadActionClient) uint64 {
	dh.locker.Lock()
	defer dh.locker.Unlock()
	dh.generation = PeerMgr.bindPeerStream(dh.peer, peerLoadStream, nil)
	dh.mode, dh.grpcClient, dh.grpcServer = HandlerModeClient, stream, nil
	return dh.generation
}
func (dh *LoadActionHandler) IsClient() bool {
	dh.locker.Lock()
	defer dh.locker.Unlock()
	return dh.mode == HandlerModeClient
}
func (dh *LoadActionHandler) receiveServer(name string, stream pb.Worker_LoadActionServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	generation := dh.setServer(stream, cancel)
	if generation == 0 {
		return status.Error(codes.FailedPrecondition, "peer registration was replaced")
	}
	done := make(chan struct{})
	go func() { dh.recv(name, dh.peer, generation, HandlerModeServer, stream, nil); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
	return nil
}
func (dh *LoadActionHandler) RecvHandler(name string) {
	dh.locker.Lock()
	owner, generation, mode, server, client := dh.peer, dh.generation, dh.mode, dh.grpcServer, dh.grpcClient
	dh.locker.Unlock()
	dh.recv(name, owner, generation, mode, server, client)
}

func (dh *LoadActionHandler) recv(name string, owner *PeerClient, generation uint64, mode byte, server pb.Worker_LoadActionServer, client pb.Worker_LoadActionClient) {
	logrus.Infof("load action recv handler setup: %s", name)
	loadBl := LoadGraphBl{}
	if mode == HandlerModeServer {
		for {
			resp, err := server.Recv()
			if err != nil {
				logrus.Errorf("recv load action error: %s", err)
				PeerMgr.RemovePeerStream(owner, peerLoadStream, generation)
				return
			}
			//logrus.Infof("LoadActionHandler server recv action: %v, end: %v", resp.Action, resp.End)
			if resp.Action == pb.LoadAction_LoadVertex {
				go loadBl.RecvVertex(
					resp.TaskId,
					resp.WorkerName,
					resp.Count,
					resp.End,
					resp.Num,
					resp.Data)
			} else if resp.Action == pb.LoadAction_LoadScatter {
				go loadBl.GatherVertex(
					resp.TaskId,
					resp.WorkerName,
					resp.End,
					resp.Num,
					resp.Data)
			} else if resp.Action == pb.LoadAction_LoadEdge {
				go loadBl.RecvEdge(
					resp.TaskId,
					resp.WorkerName,
					resp.Count,
					resp.End,
					resp.Num,
					resp.Data)
			} else if resp.Action == pb.LoadAction_LoadOutDegree {
				go loadBl.GatherOutDegree(
					resp.TaskId,
					resp.WorkerName,
					resp.End,
					resp.Data)
			}
		}
	} else if mode == HandlerModeClient {
		for {
			resp, err := client.Recv()
			if err != nil {
				logrus.Errorf("recv load action error: %s", err)
				PeerMgr.RemovePeerStream(owner, peerLoadStream, generation)
				return
			}
			//logrus.Infof("LoadActionHandler client recv action: %v, end: %v", resp.Action, resp.End)
			if resp.Action == pb.LoadAction_LoadVertex {
				go loadBl.RecvVertex(
					resp.TaskId,
					resp.WorkerName,
					resp.Count,
					resp.End,
					resp.Num,
					resp.Data)
			} else if resp.Action == pb.LoadAction_LoadScatter {
				go loadBl.GatherVertex(
					resp.TaskId,
					resp.WorkerName,
					resp.End,
					resp.Num,
					resp.Data)
			} else if resp.Action == pb.LoadAction_LoadEdge {
				go loadBl.RecvEdge(
					resp.TaskId,
					resp.WorkerName,
					resp.Count,
					resp.End,
					resp.Num,
					resp.Data)
			} else if resp.Action == pb.LoadAction_LoadOutDegree {
				go loadBl.GatherOutDegree(
					resp.TaskId,
					resp.WorkerName,
					resp.End,
					resp.Data)
			}
		}
	}
}

func (dh *LoadActionHandler) LoadAction(taskId int32, action pb.LoadAction, count int32, end bool, endNum int32, data []byte) {
	dh.locker.Lock()
	defer dh.locker.Unlock()
	if dh.mode == HandlerModeServer {
		req := pb.LoadActionResp{
			TaskId:     taskId,
			Data:       data,
			Count:      count,
			Action:     action,
			End:        end,
			Num:        endNum,
			WorkerName: ServiceWorker.WorkerName,
		}
		err := dh.grpcServer.Send(&req)
		if err != nil {
			logrus.Errorf("send do action error: %s", err)
		}
		//logrus.Infof("LoadActionHandler server send action: %v", action)
	} else if dh.mode == HandlerModeClient {
		req := pb.LoadActionReq{
			TaskId:     taskId,
			Data:       data,
			Count:      count,
			Action:     action,
			End:        end,
			Num:        endNum,
			WorkerName: ServiceWorker.WorkerName,
		}
		err := dh.grpcClient.Send(&req)
		if err != nil {
			logrus.Errorf("send do action error: %s", err)
		}
		//logrus.Infof("LoadActionHandler client send action: %v", action)
	}
}

type ScatterHandler struct {
	mode       byte
	peer       *PeerClient
	generation uint64
	grpcServer pb.Worker_ScatterServer
	grpcClient pb.Worker_ScatterClient
	locker     sync.Mutex
}

func (sh *ScatterHandler) SetServer(stream pb.Worker_ScatterServer) uint64 {
	return sh.setServer(stream, nil)
}
func (sh *ScatterHandler) setServer(stream pb.Worker_ScatterServer, cancel context.CancelFunc) uint64 {
	sh.locker.Lock()
	defer sh.locker.Unlock()
	sh.generation = PeerMgr.bindPeerStream(sh.peer, peerScatterStream, cancel)
	sh.mode, sh.grpcServer, sh.grpcClient = HandlerModeServer, stream, nil
	return sh.generation
}
func (sh *ScatterHandler) SetClient(stream pb.Worker_ScatterClient) uint64 {
	sh.locker.Lock()
	defer sh.locker.Unlock()
	sh.generation = PeerMgr.bindPeerStream(sh.peer, peerScatterStream, nil)
	sh.mode, sh.grpcClient, sh.grpcServer = HandlerModeClient, stream, nil
	return sh.generation
}
func (sh *ScatterHandler) IsClient() bool {
	sh.locker.Lock()
	defer sh.locker.Unlock()
	return sh.mode == HandlerModeClient
}
func (sh *ScatterHandler) receiveServer(name string, stream pb.Worker_ScatterServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	generation := sh.setServer(stream, cancel)
	if generation == 0 {
		return status.Error(codes.FailedPrecondition, "peer registration was replaced")
	}
	done := make(chan struct{})
	go func() { sh.recv(name, sh.peer, generation, HandlerModeServer, stream, nil); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
	return nil
}
func (sh *ScatterHandler) RecvHandler(name string) {
	sh.locker.Lock()
	owner, generation, mode, server, client := sh.peer, sh.generation, sh.mode, sh.grpcServer, sh.grpcClient
	sh.locker.Unlock()
	sh.recv(name, owner, generation, mode, server, client)
}

func (sh *ScatterHandler) recv(name string, owner *PeerClient, generation uint64, mode byte, server pb.Worker_ScatterServer, client pb.Worker_ScatterClient) {
	logrus.Infof("scatter recv handler setup: %s", name)
	if mode == HandlerModeServer {
		for {
			resp, err := server.Recv()
			if err != nil {
				logrus.Errorf("recv scatter error: %s", err)
				PeerMgr.RemovePeerStream(owner, peerScatterStream, generation)
				return
			}
			cb := ComputeBl{}
			go cb.RecvScatter(resp.TaskId, resp.Data, resp.End, resp.SIdx)
		}
	} else if mode == HandlerModeClient {
		for {
			resp, err := client.Recv()
			if err != nil {
				logrus.Errorf("recv scatter error: %s", err)
				PeerMgr.RemovePeerStream(owner, peerScatterStream, generation)
				return
			}
			cb := ComputeBl{}
			go cb.RecvScatter(resp.TaskId, resp.Data, resp.End, resp.SIdx)
		}
	}
}

func (sh *ScatterHandler) SendScatter(taskId int32, step int32, count int32, end bool, sIdx int32, data []byte) {
	sh.locker.Lock()
	defer sh.locker.Unlock()
	if sh.mode == HandlerModeServer {
		req := pb.ScatterResp{
			TaskId:     taskId,
			Step:       step,
			Data:       data,
			Count:      count,
			End:        end,
			SIdx:       sIdx,
			WorkerName: ServiceWorker.WorkerName,
		}
		err := sh.grpcServer.Send(&req)
		if err != nil {
			logrus.Errorf("send scatter error: %s", err)
		}
	} else if sh.mode == HandlerModeClient {
		req := pb.ScatterReq{
			TaskId:     taskId,
			Step:       step,
			Data:       data,
			Count:      count,
			End:        end,
			SIdx:       sIdx,
			WorkerName: ServiceWorker.WorkerName,
		}
		err := sh.grpcClient.Send(&req)
		if err != nil {
			logrus.Errorf("send scatter error: %s", err)
		}
	}
}

type StepEndHandler struct {
	mode       byte
	peer       *PeerClient
	generation uint64
	grpcServer pb.Worker_StepEndServer
	grpcClient pb.Worker_StepEndClient
	locker     sync.Mutex
}

func (sh *StepEndHandler) SetServer(stream pb.Worker_StepEndServer) uint64 {
	return sh.setServer(stream, nil)
}
func (sh *StepEndHandler) setServer(stream pb.Worker_StepEndServer, cancel context.CancelFunc) uint64 {
	sh.locker.Lock()
	defer sh.locker.Unlock()
	sh.generation = PeerMgr.bindPeerStream(sh.peer, peerStepStream, cancel)
	sh.mode, sh.grpcServer, sh.grpcClient = HandlerModeServer, stream, nil
	return sh.generation
}
func (sh *StepEndHandler) SetClient(stream pb.Worker_StepEndClient) uint64 {
	sh.locker.Lock()
	defer sh.locker.Unlock()
	sh.generation = PeerMgr.bindPeerStream(sh.peer, peerStepStream, nil)
	sh.mode, sh.grpcClient, sh.grpcServer = HandlerModeClient, stream, nil
	return sh.generation
}
func (sh *StepEndHandler) IsClient() bool {
	sh.locker.Lock()
	defer sh.locker.Unlock()
	return sh.mode == HandlerModeClient
}
func (sh *StepEndHandler) receiveServer(name string, stream pb.Worker_StepEndServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	generation := sh.setServer(stream, cancel)
	if generation == 0 {
		return status.Error(codes.FailedPrecondition, "peer registration was replaced")
	}
	done := make(chan struct{})
	go func() { sh.recv(name, sh.peer, generation, HandlerModeServer, stream, nil); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
	return nil
}
func (sh *StepEndHandler) RecvHandler(name string) {
	sh.locker.Lock()
	owner, generation, mode, server, client := sh.peer, sh.generation, sh.mode, sh.grpcServer, sh.grpcClient
	sh.locker.Unlock()
	sh.recv(name, owner, generation, mode, server, client)
}

func (sh *StepEndHandler) recv(name string, owner *PeerClient, generation uint64, mode byte, server pb.Worker_StepEndServer, client pb.Worker_StepEndClient) {
	logrus.Infof("step end recv handler setup: %s", name)
	if mode == HandlerModeServer {
		for {
			resp, err := server.Recv()
			if err != nil {
				logrus.Errorf("recv step end error: %s", err)
				PeerMgr.RemovePeerStream(owner, peerStepStream, generation)
				return
			}
			cb := ComputeBl{}
			go cb.StepEnd(resp.TaskId, resp.WorkerName)
		}
	} else if mode == HandlerModeClient {
		for {
			resp, err := client.Recv()
			if err != nil {
				logrus.Errorf("recv step end error: %s", err)
				PeerMgr.RemovePeerStream(owner, peerStepStream, generation)
				return
			}
			cb := ComputeBl{}
			go cb.StepEnd(resp.TaskId, resp.WorkerName)
		}
	}
}

func (sh *StepEndHandler) SendStepEnd(taskId int32) {
	sh.locker.Lock()
	defer sh.locker.Unlock()
	if sh.mode == HandlerModeServer {
		req := pb.StepEndResp{
			TaskId:     taskId,
			WorkerName: ServiceWorker.WorkerName,
		}
		err := sh.grpcServer.Send(&req)
		if err != nil {
			logrus.Errorf("send scatter error: %s", err)
		}
	} else if sh.mode == HandlerModeClient {
		req := pb.StepEndReq{
			TaskId:     taskId,
			WorkerName: ServiceWorker.WorkerName,
		}
		err := sh.grpcClient.Send(&req)
		if err != nil {
			logrus.Errorf("send scatter error: %s", err)
		}
	}
}

type SettingActionHandler struct {
	mode       byte
	peer       *PeerClient
	generation uint64
	grpcServer pb.Worker_SettingActionServer
	grpcClient pb.Worker_SettingActionClient
	locker     sync.Mutex
}

func (dh *SettingActionHandler) SetServer(stream pb.Worker_SettingActionServer) uint64 {
	return dh.setServer(stream, nil)
}
func (dh *SettingActionHandler) setServer(stream pb.Worker_SettingActionServer, cancel context.CancelFunc) uint64 {
	dh.locker.Lock()
	defer dh.locker.Unlock()
	dh.generation = PeerMgr.bindPeerStream(dh.peer, peerSettingStream, cancel)
	dh.mode, dh.grpcServer, dh.grpcClient = HandlerModeServer, stream, nil
	return dh.generation
}
func (dh *SettingActionHandler) SetClient(stream pb.Worker_SettingActionClient) uint64 {
	dh.locker.Lock()
	defer dh.locker.Unlock()
	dh.generation = PeerMgr.bindPeerStream(dh.peer, peerSettingStream, nil)
	dh.mode, dh.grpcClient, dh.grpcServer = HandlerModeClient, stream, nil
	return dh.generation
}
func (dh *SettingActionHandler) IsClient() bool {
	dh.locker.Lock()
	defer dh.locker.Unlock()
	return dh.mode == HandlerModeClient
}
func (dh *SettingActionHandler) receiveServer(name string, stream pb.Worker_SettingActionServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	generation := dh.setServer(stream, cancel)
	if generation == 0 {
		return status.Error(codes.FailedPrecondition, "peer registration was replaced")
	}
	done := make(chan struct{})
	go func() { dh.recv(name, dh.peer, generation, HandlerModeServer, stream, nil); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
	return nil
}
func (dh *SettingActionHandler) RecvHandler(name string) {
	dh.locker.Lock()
	owner, generation, mode, server, client := dh.peer, dh.generation, dh.mode, dh.grpcServer, dh.grpcClient
	dh.locker.Unlock()
	dh.recv(name, owner, generation, mode, server, client)
}

func (dh *SettingActionHandler) recv(name string, owner *PeerClient, generation uint64, mode byte, server pb.Worker_SettingActionServer, client pb.Worker_SettingActionClient) {
	logrus.Infof("load action recv handler setup: %s", name)
	settingBl := SettingBl{}
	if mode == HandlerModeServer {
		for {
			resp, err := server.Recv()
			if err != nil {
				logrus.Errorf("recv setting action error: %s", err)
				PeerMgr.RemovePeerStream(owner, peerSettingStream, generation)
				return
			}
			//logrus.Infof("LoadActionHandler server recv action: %v, end: %v", resp.Action, resp.End)
			if resp.Action == pb.SettingAction_SetOutEdges {
				go settingBl.GatherOutEdges(
					resp.TaskId,
					resp.WorkerName,
					resp.End,
					resp.Num,
					resp.Data)
			} else if resp.Action == pb.SettingAction_SetOutDegree {
				go settingBl.GatherOutDegree(
					resp.TaskId,
					resp.WorkerName,
					resp.End,
					resp.Num,
					resp.Data)
			}
		}
	} else if mode == HandlerModeClient {
		for {
			resp, err := client.Recv()
			if err != nil {
				logrus.Errorf("recv setting action error: %s", err)
				PeerMgr.RemovePeerStream(owner, peerSettingStream, generation)
				return
			}
			//logrus.Infof("LoadActionHandler client recv action: %v, end: %v", resp.Action, resp.End)
			if resp.Action == pb.SettingAction_SetOutEdges {
				go settingBl.GatherOutEdges(
					resp.TaskId,
					resp.WorkerName,
					resp.End,
					resp.Num,
					resp.Data)
			} else if resp.Action == pb.SettingAction_SetOutDegree {
				go settingBl.GatherOutDegree(
					resp.TaskId,
					resp.WorkerName,
					resp.End,
					resp.Num,
					resp.Data)
			}
		}
	}
}

func (dh *SettingActionHandler) SettingAction(taskId int32, action pb.SettingAction, end bool, endNum int32, data []byte) {
	dh.locker.Lock()
	defer dh.locker.Unlock()
	if dh.mode == HandlerModeServer {
		req := pb.SettingActionResp{
			TaskId:     taskId,
			Data:       data,
			Action:     action,
			End:        end,
			Num:        endNum,
			WorkerName: ServiceWorker.WorkerName,
		}
		err := dh.grpcServer.Send(&req)
		if err != nil {
			logrus.Errorf("send do action error: %s", err)
		}
		//logrus.Infof("LoadActionHandler server send action: %v", action)
	} else if dh.mode == HandlerModeClient {
		req := pb.SettingActionReq{
			TaskId:     taskId,
			Data:       data,
			Action:     action,
			End:        end,
			Num:        endNum,
			WorkerName: ServiceWorker.WorkerName,
		}
		err := dh.grpcClient.Send(&req)
		if err != nil {
			logrus.Errorf("send do action error: %s", err)
		}
		//logrus.Infof("LoadActionHandler client send action: %v", action)
	}
}
