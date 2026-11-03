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
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
	"vermeer/apps/common"
	"vermeer/apps/compute"
	"vermeer/apps/graphio"
	"vermeer/apps/master/schedules"
	"vermeer/apps/master/threshold"
	"vermeer/apps/master/workers"
	pb "vermeer/apps/protos"
	"vermeer/apps/structure"
	"vermeer/apps/version"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

type ServerHandler struct {
	pb.UnimplementedMasterServer
	locker sync.Mutex
}

func (h *ServerHandler) Init() {
	h.locker = sync.Mutex{}
}

func (h *ServerHandler) SayHelloMaster(ctx context.Context, req *pb.HelloMasterReq) (*pb.HelloMasterResp, error) {
	h.locker.Lock()
	defer h.locker.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p, _ := peer.FromContext(ctx)

	// check worker version, if not match, deny
	if req.Version != version.Version {
		err := fmt.Errorf("registration denied: inconsistent registration version! master version %v, worker version %v", version.Version, req.Version)
		logrus.Errorf(err.Error())
		return &pb.HelloMasterResp{Base: &pb.BaseResponse{ErrorCode: -1, Message: err.Error()}}, err
	}

	semi := strings.LastIndex(p.Addr.String(), ":")
	if semi < 0 {
		err := fmt.Errorf("worker grpc peer error: %s", req.WorkerPeer)
		logrus.Errorf(err.Error())
		return &pb.HelloMasterResp{Base: &pb.BaseResponse{ErrorCode: -1, Message: err.Error()}}, err
	}
	ip := p.Addr.String()[:semi]

	semi = strings.LastIndex(req.GetWorkerPeer(), ":")
	if semi < 0 {
		err := fmt.Errorf("worker grpc peer error: %s", req.WorkerPeer)
		logrus.Errorf(err.Error())
		return &pb.HelloMasterResp{Base: &pb.BaseResponse{ErrorCode: -1, Message: err.Error()}}, err
	}
	port := req.GetWorkerPeer()[semi+1:]

	workers := workerMgr.GetAllWorkers()
	var existWorkerName string
	for _, worker := range workers {
		if workerMgr.IsAbandonedRegistration(worker) {
			workerBl.KickOfflineSession(worker)
			continue
		}
		if worker.GrpcPeer == ip+":"+port {
			existWorkerName = worker.Name
			break
		}
	}
	if existWorkerName != "" {
		//如果worker已经存在，必须等待至grpc recv handler感知后，删除worker
		for existing := workerMgr.GetWorker(existWorkerName); existing != nil; existing = workerMgr.GetWorker(existWorkerName) {
			if workerMgr.CanReplaceDisconnectedSession(existing) {
				workerBl.KickOfflineSession(existing)
				continue
			}
			logrus.Infof("worker %v exist, wait one second", existWorkerName)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Second):
			}
		}
	}

	reqWorker, err := workerMgr.CreateWorker(ip+":"+port, ip, req.Version, req.WorkerGroup)
	if err != nil {
		logrus.Errorf("failed to create a WorkerClient, error: %s", err)
		return &pb.HelloMasterResp{WorkerId: -1, WorkerName: reqWorker.Name}, err
	}

	_, err = workerMgr.AddWorker(reqWorker)
	if err != nil {
		logrus.Errorf("failed to add a WorkerClient to the WorkerManager, error: %s", err)
		return &pb.HelloMasterResp{}, err
	}
	registered := false
	defer func() {
		if !registered {
			workerMgr.RemoveWorkerSession(reqWorker)
			if reqWorker.Connection != nil {
				_ = reqWorker.Connection.Close()
			}
		}
	}()

	_, err = Scheduler.ChangeWorkerStatus(reqWorker.Name, schedules.WorkerOngoingStatusIdle)
	if err != nil {
		logrus.Errorf("failed to change worker status to idle, error: %s", err)
		return &pb.HelloMasterResp{}, err
	}

	logrus.Infof("worker say hello name: %s and set to workgroup: %s, client: %s", reqWorker.Name, reqWorker.Group, p.Addr.String())

	resp := pb.HelloMasterResp{
		WorkerId:   reqWorker.Id,
		WorkerName: reqWorker.Name,
	}

	// connect to client
	workerClient := workerMgr.GetWorker(reqWorker.Name)
	dialOptions := grpc.WithDefaultCallOptions(
		grpc.MaxCallSendMsgSize(1*1024*1024*1024),
		grpc.MaxCallRecvMsgSize(1*1024*1024*1024))

	connection, err := common.DialGRPCPeer(ctx,
		ip+":"+port, grpc.WithTransportCredentials(insecure.NewCredentials()), dialOptions)
	if err != nil {
		logrus.Errorf("connect worker error: %s", err)
		return &resp, err
	}
	workerClient.Session = pb.NewWorkerClient(connection)
	workerMgr.SetWorkerConnection(workerClient, connection)
	spaces := structure.SpaceManager.GetAllSpace()
	for _, space := range spaces {
		resp.Spaces = append(resp.Spaces, space.Name())
	}

	initWorkerClient(ctx, workerClient)
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	graphs := graphMgr.GetGraphsByWorker(reqWorker.Name)
	taskCanRecovery := make([]*structure.TaskInfo, 0)
	for _, graph := range graphs {
		if graph.State == structure.GraphStateInComplete {
			workerAllAlive := true
			for _, worker := range graph.Workers {
				if !workerMgr.CheckWorkerAlive(worker.Name) {
					workerAllAlive = false
					break
				}
			}
			if workerAllAlive {
				graph.SetState(structure.GraphStateOnDisk) // InComplete is not a persistent state
				tasks := taskMgr.GetTaskByGraph(graph.SpaceName, graph.Name)
				for _, task := range tasks {
					if task.State == structure.TaskStateWaiting {
						logrus.Infof("waiting taskid:%v", task.ID)
						taskCanRecovery = append(taskCanRecovery, task)
					} else if task.State != structure.TaskStateComplete && task.State != structure.TaskStateLoaded && task.State != structure.TaskStateError {
						// task.SetErrMsg(fmt.Errorf("recovery task error, task state:%v", task.State).Error())
						// task.SetState(structure.TaskStateError)
						taskMgr.SetError(task, fmt.Errorf("recovery task error, task state:%v", task.State).Error())
						err := taskMgr.FinishTask(task.ID)
						if err != nil {
							logrus.Errorf("finish task error: %s", err)
						}
					}
				}
			}
		}
	}
	go func() {
		sort.Slice(taskCanRecovery, func(i, j int) bool {
			return taskCanRecovery[i].ID < taskCanRecovery[j].ID
		})
		time.Sleep(3 * time.Second)
		for _, task := range taskCanRecovery {
			_, err = Scheduler.QueueTask(task)
			if err != nil {
				logrus.Errorf("recovery task error:%v", err)
			}
			logrus.Infof("worker:%v recovery task:%v type:%v ", workerClient.Name, task.ID, task.Type)
		}
	}()
	for _, v := range workerMgr.GetBootstrapWorkers() {
		if v.GrpcPeer == ip+":"+port {
			logrus.Debugf("workerMgr has a duplicate address, ignore it")
			continue
		}
		workerInfo := pb.WorkerInfo{
			Name:     v.Name,
			Id:       v.Id,
			GrpcPeer: v.GrpcPeer,
		}
		resp.Workers = append(resp.Workers, &workerInfo)
	}

	registered = true
	return &resp, nil
}

func initWorkerClient(ctx context.Context, workerClient *workers.WorkerClient) {
	logrus.Infof("init worker: %s@%s", workerClient.Name, workerClient.GrpcPeer)
	// init memory limit
	err := workers.SendMemLimitContext(ctx, workerClient, &workers.WorkerMemLimit{
		MaxMem:  threshold.GroupMaxMemory(workerClient.Group),
		MinFree: threshold.GroupMinFree(workerClient.Group),
		GcRatio: float32(threshold.GroupGcPct(workerClient.Group)) / 100,
	})
	if err != nil {
		logrus.Errorf("send memory limit to worker '%s@%s' error: %s", workerClient.Name, workerClient.GrpcPeer, err)
	}
}

func (h *ServerHandler) LoadGraphTask(stream pb.Master_LoadGraphTaskServer) error {
	md, _ := metadata.FromIncomingContext(stream.Context())
	name := md.Get("worker_name")[0]
	//WorkerMgr.GetWorker(name).loadServer.SetServer(stream)
	//WorkerMgr.GetWorker(name).loadServer.RecvHandler(name)

	return ServerMgr.StartingLoadServer(name, stream)
}

func (h *ServerHandler) ComputeTask(stream pb.Master_ComputeTaskServer) error {
	md, _ := metadata.FromIncomingContext(stream.Context())
	name := md.Get("worker_name")[0]
	//WorkerMgr.GetWorker(name).computeServer.SetServer(stream)
	//WorkerMgr.GetWorker(name).computeServer.RecvHandler(name)
	return ServerMgr.StartingComputeServer(name, stream)
}

func (h *ServerHandler) SuperStep(req *pb.SuperStepReq, stream pb.Master_SuperStepServer) error {
	_ = req
	md, _ := metadata.FromIncomingContext(stream.Context())
	name := md.Get("worker_name")[0]
	//WorkerMgr.GetWorker(name).SuperStepServer.SetServer(stream)
	if err := ServerMgr.PutSuperStepServer(name, stream); err != nil {
		return err
	}
	worker := workerMgr.GetWorker(name)
	defer workerMgr.ClearTaskStream(worker, workers.SuperStepStream, stream.Context())
	<-stream.Context().Done()
	return nil
}

func (h *ServerHandler) FetchLoadPart(ctx context.Context, req *pb.FetchLoadPartReq) (*pb.FetchLoadPartResp, error) {
	_ = ctx
	resp := pb.FetchLoadPartResp{}
	loadTask := LoadTaskBl{}
	partition := loadTask.FetchPartition(req.TaskId, req.WorkerName)
	resp.PartId = partition.Id
	resp.TaskId = req.TaskId
	resp.Params = partition.Params
	return &resp, nil
}

func (h *ServerHandler) LoadPartStatus(
	ctx context.Context, req *pb.LoadPartStatusReq) (*pb.LoadPartStatusResp, error) {
	_ = ctx
	resp := pb.LoadPartStatusResp{}
	loadTaskBl := LoadTaskBl{}
	loadTaskBl.LoadPartStatus(req.TaskId, req.PartId, req.State)
	return &resp, nil
}

func (h *ServerHandler) WorkVertexCount(
	ctx context.Context,
	req *pb.WorkerVertexCountReq) (*pb.WorkerVertexCountResp, error) {
	_ = ctx
	resp := pb.WorkerVertexCountResp{}
	loadTaskBl := LoadTaskBl{}
	loadTaskBl.WorkerVertexCount(req.TaskId, req.WorkerName, req.Count)
	return &resp, nil
}

func (h *ServerHandler) WorkEdgeCount(
	ctx context.Context,
	req *pb.WorkerEdgeCountReq) (*pb.WorkerEdgeCountResp, error) {
	_ = ctx
	resp := pb.WorkerEdgeCountResp{}
	loadTaskBl := LoadTaskBl{}
	loadTaskBl.WorkerEdgeCount(req.TaskId, req.WorkerName, req.Count)
	return &resp, nil
}

func (h *ServerHandler) GetGraphWorkers(
	ctx context.Context, req *pb.GetGraphWorkersReq) (*pb.GetGraphWorkersResp, error) {
	_ = ctx
	resp := pb.GetGraphWorkersResp{}
	loadTaskBl := LoadTaskBl{}
	gw := loadTaskBl.GetGraphWorkers(req.GetSpaceName(), req.GraphName)
	resp.Workers = make([]*pb.GraphWorker, 0, len(gw))
	for _, v := range gw {
		w := pb.GraphWorker{
			Name:        v.Name,
			VertexCount: v.VertexCount,
			VertIdStart: v.VertIdStart,
		}
		resp.Workers = append(resp.Workers, &w)
	}
	return &resp, nil
}

func (h *ServerHandler) LoadTaskStatus(
	ctx context.Context, req *pb.LoadTaskStatusReq) (*pb.LoadTaskStatusResp, error) {
	h.locker.Lock()
	defer h.locker.Unlock()
	_ = ctx
	resp := pb.LoadTaskStatusResp{}

	loadTaskBl := LoadTaskBl{}
	loadTaskBl.LoadTaskStatus(req.TaskId, req.State, req.WorkerName, req.ErrorMsg)

	return &resp, nil
}

func (h *ServerHandler) ComputeTaskStatus(
	ctx context.Context, req *pb.ComputeTaskStatusReq) (*pb.ComputeTaskStatusResp, error) {
	_ = ctx
	h.locker.Lock()
	defer h.locker.Unlock()
	resp := pb.ComputeTaskStatusResp{}

	ctb := ComputeTaskBl{}
	ctb.ComputeTaskStatus(req.TaskId, req.State, req.WorkerName, req.Step, req.ComputeValues, req.ErrorMsg)

	return &resp, nil
}

func (h *ServerHandler) SettingGraph(
	ctx context.Context, req *pb.SettingGraphReq) (*pb.SettingGraphResp, error) {
	_ = ctx
	h.locker.Lock()
	defer h.locker.Unlock()
	resp := pb.SettingGraphResp{}

	ctb := ComputeTaskBl{}
	ctb.SettingGraphStatus(req.TaskId, req.State, req.WorkerName, req.ErrorMsg)

	return &resp, nil
}

func (h *ServerHandler) UploadVertexValue(ctx context.Context, in *pb.UploadVertexValueReq) (*pb.UploadVertexValueResp, error) {
	_ = ctx
	h.locker.Lock()
	defer h.locker.Unlock()
	computeTask := computerTaskMgr.GetTask(in.TaskId)
	for _, vertexValue := range in.GetVertexValues() {
		computeTask.ComputeValue.Values = append(computeTask.ComputeValue.Values,
			compute.VertexValue{ID: vertexValue.ID, Value: vertexValue.Value})
	}
	//更新时间
	if computeTask.ComputeValue.Timer != nil {
		computeTask.ComputeValue.Timer.Stop()
	}
	computerTaskMgr.InitTimer(computeTask.Task.ID)
	return &pb.UploadVertexValueResp{}, nil
}

func (h *ServerHandler) UploadStatistics(ctx context.Context, in *pb.UploadStatisticsReq) (*pb.UploadStatisticsResp, error) {
	_ = ctx
	h.locker.Lock()
	defer h.locker.Unlock()
	computeTask := computerTaskMgr.GetTask(in.TaskId)
	if computeTask.Statistics == nil {
		computeTask.Statistics = make([][]byte, 0)
	}
	computeTask.Statistics = append(computeTask.Statistics, in.GetStatistics())
	return &pb.UploadStatisticsResp{}, nil
}

func (h *ServerHandler) UploadTPResult(ctx context.Context, in *pb.UploadTPResultReq) (*pb.UploadTPResultResp, error) {
	_ = ctx
	h.locker.Lock()
	defer h.locker.Unlock()
	computeTask := computerTaskMgr.GetTask(in.TaskId)
	if computeTask.TpResult.WorkerResult == nil {
		computeTask.TpResult.WorkerResult = make([][]byte, 0)
	}
	computeTask.TpResult.WorkerResult = append(computeTask.TpResult.WorkerResult, in.GetResult())
	return &pb.UploadTPResultResp{}, nil
}

type LoadGraphServer struct {
	streamServer pb.Master_LoadGraphTaskServer
	worker       *workers.WorkerClient
	sendMutex    sync.Mutex
}

func (s *LoadGraphServer) SetServer(stream pb.Master_LoadGraphTaskServer) {
	s.streamServer = stream
}

func (s *LoadGraphServer) RecvHandler(workerName string) {
	logrus.Infof("load graph task recv handler setup: %s", workerName)
	worker := s.worker
	for {
		req, err := s.streamServer.Recv()
		if err != nil {
			logrus.Errorf("load graph task recv: %s", err)
			workerMgr.ClearTaskStream(worker, workers.LoadTaskStream, s.streamServer.Context())
			time.Sleep(3 * time.Second)

			workerBl.KickOfflineSession(worker)
			return
		}
		if req.State == graphio.LoadPartStatusPrepared {
		} else if req.State == graphio.LoadPartStatusDone {

		}
	}
}

type LoadingTaskReq struct {
	TaskId    int32
	Step      pb.LoadStep
	LoadType  string
	GraphName string
	SpaceName string
	Workers   []string
	Params    map[string]string
}

func (s *LoadGraphServer) SendLoadReq(req *LoadingTaskReq) error {
	return s.AsyncLoad(req.TaskId, req.Step, req.LoadType, req.GraphName, req.SpaceName, req.Workers, req.Params)
}

func (s *LoadGraphServer) AsyncLoad(
	taskId int32,
	step pb.LoadStep,
	loadType string,
	graphName string,
	spaceName string,
	workers []string,
	params map[string]string) error {
	// Dispatch, phase changes and error cleanup share this stream.
	s.sendMutex.Lock()
	defer s.sendMutex.Unlock()
	err := s.streamServer.Send(&pb.LoadGraphTaskResp{
		Base:      &pb.BaseResponse{},
		LoadType:  loadType,
		TaskId:    taskId,
		Params:    params,
		Step:      step,
		GraphName: graphName,
		SpaceName: spaceName,
		Workers:   workers,
	})
	if err != nil {
		logrus.Errorf("send load graph task error: %s", err)
		return err
	}
	return nil
}

type ComputeTaskServer struct {
	streamServer pb.Master_ComputeTaskServer
	worker       *workers.WorkerClient
}

func (s *ComputeTaskServer) SetServer(stream pb.Master_ComputeTaskServer) {
	s.streamServer = stream
}

func (s *ComputeTaskServer) RecvHandler(workerName string) {
	logrus.Infof("compute task recv handler setup: %s", workerName)
	worker := s.worker
	for {
		req, err := s.streamServer.Recv()
		if err != nil {
			logrus.Errorf("compute task recv error: %s", err)
			workerMgr.ClearTaskStream(worker, workers.ComputeTaskStream, s.streamServer.Context())
			time.Sleep(3 * time.Second)

			workerBl.KickOfflineSession(worker)
			return
		}
		_ = req
	}
}

func (s *ComputeTaskServer) AsyncCompute(
	algorithm string, graphName string, spaceName string, taskId int32, params map[string]string, action pb.ComputeAction) error {
	err := s.streamServer.Send(&pb.ComputeTaskResp{
		Base:      &pb.BaseResponse{},
		TaskId:    taskId,
		Algorithm: algorithm,
		GraphName: graphName,
		SpaceName: spaceName,
		Params:    params,
		Action:    action,
	})
	if err != nil {
		logrus.Errorf("send async compute task error: %s", err)
		return err
	}
	return nil
}

type SuperStepServer struct {
	streamServer pb.Master_SuperStepServer
}

func (s *SuperStepServer) SetServer(stream pb.Master_SuperStepServer) {
	s.streamServer = stream
}

//func (s *SuperStepServer) RecvHandler(workerName string) {
//	logrus.Infof("super step recv handler setup: %s", workerName)
//	for {
//		req, err := s.streamServer.Recv()
//		if err != nil {
//			logrus.Errorf("super step recv: %s", err)
//			time.Sleep(3 * time.Second)
//			continue
//		}
//		_ = req
//	}
//}

func (s *SuperStepServer) AsyncSuperStep(
	taskId int32, step int32, output bool, computeValues map[string][]byte) {
	err := s.streamServer.Send(&pb.SuperStepResp{
		Base:          &pb.BaseResponse{},
		TaskId:        taskId,
		Step:          step,
		ComputeValues: computeValues,
		Output:        output,
	})
	if err != nil {
		logrus.Errorf("AsyncSuperStep error: %s", err)
	}
}
