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
	"sync"
	"sync/atomic"
	"time"
	"vermeer/apps/common"
	"vermeer/apps/compute"
	"vermeer/apps/graphio"
	"vermeer/apps/options"
	pb "vermeer/apps/protos"
	"vermeer/apps/serialize"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"
)

type ComputeBl struct {
}

func (cb *ComputeBl) initComputeTask(taskID int32, spaceName string, graphName string, params map[string]string) (*compute.ComputerTask, error) {
	graph := GraphMgr.GetGraphByName(spaceName, graphName)
	if graph == nil {
		return nil, fmt.Errorf("graph not eixist: %s", graphName)
	}
	task, err := TaskMgr.CreateTask(spaceName, structure.TaskTypeCompute, taskID)
	if err != nil {
		return nil, fmt.Errorf("worker computer init error:%w", err)
	}
	task.GraphName = graphName
	task.Params = params
	for _, w := range graph.Workers {
		tw := structure.TaskWorker{
			Name: w.Name,
		}
		task.Workers = append(task.Workers, &tw)
	}
	_, err = TaskMgr.AddTask(task)
	if err != nil {
		return nil, err
	}

	logrus.Infof("create compute task: %d, graph: %s", taskID, graphName)
	computeTask := ComputeTaskMgr.MakeTask(task)
	parallel := int32(options.GetInt(params, "compute.parallel"))
	if parallel <= 0 {
		logrus.Infof("compute.parallel value must be larger than 0, get: %v, set to defalut value :1", parallel)
		parallel = 1
	}
	computeTask.Parallel = &parallel
	return computeTask, nil
}

func (cb *ComputeBl) StartCompute(taskID int32, spaceName string, graphName string, algorithm string, params map[string]string) {
	defer func() {
		if r := recover(); r != nil {
			cb.SetStatusError(taskID, fmt.Sprintf("StartCompute panic recover panic:%v, stack message: %s", r,
				common.GetCurrentGoroutineStack()))
			logrus.Errorf("StartCompute panic recover taskID:%v, panic:%v, stack message: %s", taskID, r,
				common.GetCurrentGoroutineStack())
		}
	}()
	var err error
	ct := ComputeTaskMgr.GetTask(taskID)
	if ct == nil {
		ct, err = cb.initComputeTask(taskID, spaceName, graphName, params)
		if err != nil {
			logrus.Errorf("init compute task error:%v", err)
			cb.SetStatusError(taskID, fmt.Sprintf("init compute task error:%v", err))
			return
		}
	}

	ct.StepWg.Add(1)
	parallel := options.GetInt(params, "compute.parallel")
	if parallel <= 0 {
		logrus.Infof("compute.parallel value must be larger than 0, get: %v, set to defalut value :1", parallel)
		parallel = 1
	}
	workerComputer := AlgorithmMgr.MakeWorkerComputer(algorithm)
	ct.ComputeWorker = workerComputer
	ctx := workerComputer.MakeContext(params)
	ctx.Parallel = parallel
	graph := GraphMgr.GetGraphByName(spaceName, graphName)
	if graph == nil {
		logrus.Errorf("graph not eixist: %s", graphName)
		cb.SetStatusError(taskID, fmt.Sprintf("graph not eixist: %s", graphName))
		return
	}
	ctx.GraphData = graph.Data
	ctx.Workers = len(graph.Workers)
	ctx.MakeSendBuffer()
	ctx.WorkerIdx = graph.GetSelfIndex()
	err = workerComputer.Init()
	if err != nil {
		logrus.Errorf("worker computer init error:%v", err)
		cb.SetStatusError(taskID, fmt.Sprintf("worker computer init error:%v", err))
		return
	}

	for {
		ct.Locker.Lock()
		action := atomic.LoadInt32(&ct.Task.Action)
		if action != structure.ActionPauseTask {
			if action == structure.ActionCancelTask {
				cb.cancelAction(ct)
				ct.Locker.Unlock()
				return
			}
			ct.Task.SetState(structure.TaskStateInitOK)
			ct.WaitingForStep = true
			ct.Locker.Unlock()
			break
		}
		// Pause outside the handoff lock, then recheck before publishing readiness.
		ct.Locker.Unlock()
		if !cb.pauseAction(ct) {
			return
		}
	}
	req := pb.ComputeTaskStatusReq{
		TaskId:        taskID,
		State:         string(structure.TaskStateInitOK),
		Step:          ct.Step,
		ComputeValues: map[string][]byte{},
		WorkerName:    ServiceWorker.WorkerName,
	}
	_, err = ServiceWorker.MasterClient.ComputeTaskStatus(context.Background(), &req)
	if err != nil {
		ct.Locker.Lock()
		ct.WaitingForStep = false
		ct.Locker.Unlock()
		if cb.CheckAction(ct) {
			logrus.Errorf("ComputeTaskStatus error:%v", err)
			cb.SetStatusError(taskID, fmt.Sprintf("ComputeTaskStatus error:%v", err))
		}
	}

	//go cb.RunSuperStep(ct.Task.ID, nil)
}

func (cb *ComputeBl) RunSuperStep(taskID int32, computeValues map[string][]byte) {
	defer func() {
		if r := recover(); r != nil {
			cb.SetStatusError(taskID, fmt.Sprintf("RunSuperStep panic recover panic:%v, stack message: %s", r,
				common.GetCurrentGoroutineStack()))
			logrus.Errorf("RunSuperStep panic recover taskID:%v, panic:%v, stack message: %s", taskID, r,
				common.GetCurrentGoroutineStack())
		}
	}()
	computeTask := ComputeTaskMgr.GetTask(taskID)
	if computeTask == nil {
		return
	}
	computeTask.Locker.Lock()
	computeTask.WaitingForStep = false
	computeTask.Locker.Unlock()
	if !cb.CheckAction(computeTask) {
		return
	}
	computeTask.Step += 1
	ctx := computeTask.ComputeWorker.Context()
	ctx.Step += 1
	if computeValues != nil {
		ctx.UnmarshalValues(computeValues)
	}
	computeTask.Task.SetState(structure.TaskStateStepDoing)

	logrus.Infof("RunSuperStep start step: %d", computeTask.Step)
	computeTask.ComputeWorker.BeforeStep()
	computeTask.StepWg.Done()

	workerCount := len(computeTask.Task.Workers)
	peers := make([]*PeerClient, 0, workerCount-1)
	for _, wn := range computeTask.Task.Workers {
		if wn.Name == ServiceWorker.WorkerName {
			continue
		}
		peers = append(peers, PeerMgr.GetPeer(wn.Name))
	}

	*computeTask.Parallel = int32(ctx.Parallel) * int32(len(peers))
	computeTask.RecvWg.Add(int32(ctx.Parallel) * int32(len(peers)))

	parallel := ctx.Parallel
	partCnt := int(ctx.GraphData.VertexCount)/parallel + 1
	wg := sync.WaitGroup{}
	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go func(pId int) {
			defer func() {
				if r := recover(); r != nil {
					cb.SetStatusError(taskID, fmt.Sprintf("RunSuperStep panic recover panic:%v, stack message: %s",
						r, common.GetCurrentGoroutineStack()))
					logrus.Errorf("RunSuperStep panic recover taskID:%v, pId:%v panic:%v, stack message: %s",
						taskID, pId, r, common.GetCurrentGoroutineStack())
				}
			}()
			defer wg.Done()
			defer func() {
				for _, peer := range peers {
					peer.ScatterHandler.SendScatter(
						taskID,
						ctx.Step,
						0,
						true,
						0,
						[]byte{})
				}
			}()
			bIdx := uint32(partCnt*pId) + ctx.GraphData.VertIDStart
			eIdx := bIdx + uint32(partCnt)
			if eIdx > ctx.GraphData.VertIDStart+ctx.GraphData.VertexCount {
				eIdx = ctx.GraphData.VertIDStart + ctx.GraphData.VertexCount
			}
			vOffSet := serialize.SUint32(bIdx)
			for j := bIdx; j < eIdx; j++ {
				if j%10000 == 0 && !cb.CheckAction(computeTask) {
					//减少 check action 次数，降低影响
					return
				}
				computeTask.ComputeWorker.Compute(j, pId)
			}

			if len(peers) > 0 {
				_ = ctx.SendBuffers[pId].Marshal(&vOffSet)
				for j := bIdx; j < eIdx; j++ {
					if j%10000 == 0 && !cb.CheckAction(computeTask) {
						//减少 check action 次数，降低影响
						return
					}
					err := ctx.SendBuffers[pId].Marshal(computeTask.ComputeWorker.VertexValue(j))
					if err != nil {
						logrus.Errorf("sendbuffer marshal error:%v", err)
						cb.SetStatusError(taskID, fmt.Sprintf("sendbuffer marshal error:%v", err))
					}
					if ctx.SendBuffers[pId].Full() {
						for _, peer := range peers {
							peer.ScatterHandler.SendScatter(
								taskID,
								ctx.Step,
								int32(ctx.SendBuffers[pId].ObjCount()),
								false,
								0,
								ctx.SendBuffers[pId].PayLoad())
						}
						ctx.SendBuffers[pId].Reset()
						vOffSet = serialize.SUint32(j + 1)
						err = ctx.SendBuffers[pId].Marshal(&vOffSet)
						if err != nil {
							logrus.Errorf("sendbuffer marshal error:%v", err)
							cb.SetStatusError(taskID, fmt.Sprintf("sendbuffer marshal error:%v", err))
						}
					}
				}
			}

			for _, peer := range peers {
				peer.ScatterHandler.SendScatter(
					taskID,
					ctx.Step,
					int32(ctx.SendBuffers[pId].ObjCount()),
					false,
					0,
					ctx.SendBuffers[pId].PayLoad())
			}
			ctx.SendBuffers[pId].Reset()

			// TODO: wait for all compute done
			if len(peers) > 0 {
				for sIdx, cnt := range computeTask.ComputeWorker.Scatters() {
					if sIdx%10000 == 0 && !cb.CheckAction(computeTask) {
						//减少 check action 次数，降低影响
						return
					}
					partCnt = cnt/parallel + 1
					start := partCnt * pId
					end := cnt / parallel * (pId + 1)
					if pId+1 == parallel {
						end = computeTask.ComputeWorker.Context().PartStart(cnt) +
							computeTask.ComputeWorker.Context().PartCount(cnt)
					}

					vOffSet = serialize.SUint32(start)
					cb.WaitDone(start, end, pId, taskID, vOffSet, sIdx, peers, ctx, computeTask)

					for _, peer := range peers {
						peer.ScatterHandler.SendScatter(
							taskID,
							ctx.Step,
							int32(ctx.SendBuffers[pId].ObjCount()),
							false,
							int32(sIdx),
							ctx.SendBuffers[pId].PayLoad())
					}
					ctx.SendBuffers[pId].Reset()
				}
			}
		}(i)
	}
	wg.Wait()
	for _, peer := range peers {
		go peer.StepEndHandler.SendStepEnd(taskID)
	}

	// send to self
	go cb.StepEnd(taskID, ServiceWorker.WorkerName)
}

func (cb *ComputeBl) WaitDone(start int, end int, pId int, taskID int32, vOffSet serialize.SUint32, sIdx int,
	peers []*PeerClient, ctx *compute.CWContext, computeTask *compute.ComputerTask) {

	for i := start; i < end; i++ {
		err := ctx.SendBuffers[pId].Marshal(computeTask.ComputeWorker.ScatterValue(sIdx, i))
		if err != nil {
			logrus.Errorf("sendbuffer marshal error:%v", err)
			cb.SetStatusError(taskID, fmt.Sprintf("sendbuffer marshal error:%v", err))
		}
		if ctx.SendBuffers[pId].Full() {
			for _, peer := range peers {
				peer.ScatterHandler.SendScatter(
					taskID,
					ctx.Step,
					int32(ctx.SendBuffers[pId].ObjCount()),
					false,
					0,
					ctx.SendBuffers[pId].PayLoad())
			}
			ctx.SendBuffers[pId].Reset()
			vOffSet = serialize.SUint32(i + 1)
			err = ctx.SendBuffers[pId].Marshal(&vOffSet)
			if err != nil {
				logrus.Errorf("sendbuffer marshal error:%v", err)
				cb.SetStatusError(taskID, fmt.Sprintf("sendbuffer marshal error:%v", err))
			}
		}
	}
}

func (cb *ComputeBl) RecvScatter(taskID int32, data []byte, end bool, sIdx int32) {
	defer func() {
		if r := recover(); r != nil {
			cb.SetStatusError(taskID, fmt.Sprintf("RecvScatter panic recover panic:%v, stack message: %s", r,
				common.GetCurrentGoroutineStack()))
			logrus.Errorf("RecvScatter panic recover taskID:%v, panic:%v, stack message: %s", taskID, r,
				common.GetCurrentGoroutineStack())
		}
	}()
	_ = sIdx
	computeTask := ComputeTaskMgr.GetTask(taskID)
	for i := 0; i < 100 && computeTask == nil; i++ {
		//wait 100ms if computeTask not init.
		logrus.Warnf("RecvScatter task id:%v is not available, wait 100ms", taskID)
		time.Sleep(100 * time.Millisecond)
		computeTask = ComputeTaskMgr.GetTask(taskID)
	}
	if !cb.CheckAction(computeTask) {
		return
	}
	//defer cb.CheckAction(computeTask)
	computeTask.RecvWg.Add(1)
	computeTask.StepWg.Wait()

	i := 0
	vOffSet := serialize.SUint32(0)
	if len(data) >= 4 {
		n, _ := vOffSet.Unmarshal(data)
		i += n
	}
	for i < len(data) {
		n, err := computeTask.ComputeWorker.VertexValue(uint32(vOffSet)).Unmarshal(data[i:])
		if err != nil {
			logrus.Errorf("load graph read vertex error: %s", err)
			break
		}
		i += n
		vOffSet += 1
	}
	computeTask.RecvWg.Done()
	if end {
		computeTask.RecvWg.Done()
	}
}

func (cb *ComputeBl) StepEnd(taskID int32, workerName string) {
	defer func() {
		if r := recover(); r != nil {
			cb.SetStatusError(taskID, fmt.Sprintf("StepEnd panic recover panic:%v, stack message: %s", r,
				common.GetCurrentGoroutineStack()))
			logrus.Errorf("StepEnd panic recover taskID:%v, panic:%v, stack message: %s", taskID, r,
				common.GetCurrentGoroutineStack())
		}
	}()
	computeTask := ComputeTaskMgr.GetTask(taskID)
	for i := 0; i < 100 && computeTask == nil; i++ {
		//wait 100ms if computeTask not init.
		logrus.Warnf("StepEnd task id:%v is not available, wait 100ms", taskID)
		time.Sleep(100 * time.Millisecond)
		computeTask = ComputeTaskMgr.GetTask(taskID)
	}
	if !cb.CheckAction(computeTask) {
		return
	}
	//defer cb.CheckAction(computeTask)
	computeTask.StepWg.Wait()
	ctx := computeTask.ComputeWorker.Context()
	logrus.Infof("task:%v recv StepEnd: %s", taskID, workerName)

	// wait for all messages are processed
	computeTask.RecvWg.Wait()
	computeTask.Locker.Lock()
	defer computeTask.Locker.Unlock()
	computeTask.Task.SetWorkerState(workerName, structure.TaskStateStepDone)
	if computeTask.Task.CheckTaskState(structure.TaskStateStepDone) {
		computeTask.ComputeWorker.AfterStep()
		computeTask.Task.SetState(structure.TaskStateStepDone)
		logrus.Infof("task:%v RecvScatter end step: %d", taskID, computeTask.Step)
		computeTask.StepWg.Add(1)
		computeTask.RecvWg.Reset()
		computeValues := ctx.MarshalValues()
		req := pb.ComputeTaskStatusReq{
			TaskId:        taskID,
			State:         string(structure.TaskStateStepDone),
			Step:          computeTask.Step,
			ComputeValues: computeValues,
			WorkerName:    ServiceWorker.WorkerName,
		}
		_, err := ServiceWorker.MasterClient.ComputeTaskStatus(context.Background(), &req)
		if err != nil {
			logrus.Errorf("ComputeTaskStatus error:%v", err)
			cb.SetStatusError(taskID, fmt.Sprintf("ComputeTaskStatus error:%v", err))
		}
	}
}

func (cb *ComputeBl) RunOutput(taskID int32) {
	defer func() {
		if r := recover(); r != nil {
			cb.SetStatusError(taskID, fmt.Sprintf("RunOutput panic recover panic:%v, stack message: %s", r,
				common.GetCurrentGoroutineStack()))
			logrus.Errorf("RunOutput panic recover taskID:%v, panic:%v, stack message: %s", taskID, r,
				common.GetCurrentGoroutineStack())
		}
	}()
	computeTask := ComputeTaskMgr.GetTask(taskID)
	if !cb.CheckAction(computeTask) {
		return
	}
	if validator, ok := computeTask.ComputeWorker.(interface{ ValidateOutput() error }); ok {
		if err := validator.ValidateOutput(); err != nil {
			cb.SetStatusError(taskID, err.Error())
			return
		}
	}
	computeTask.ComputeWorker.Context().Output = true
	computeTask.Task.SetState(structure.TaskStateOutput)
	graph := GraphMgr.GetGraphByName(computeTask.Task.SpaceName, computeTask.Task.GraphName)

	parallel := options.GetInt(computeTask.Task.Params, "output.parallel")
	if parallel <= 0 {
		logrus.Infof("output.parallel value must be larger than 0, get: %v, set to defalut value :1", parallel)
		parallel = 1
	}

	maker := AlgorithmMgr.GetMaker(computeTask.Algorithm)
	if maker == nil {
		return
	}

	switch maker.Type() {
	case compute.AlgorithmOLAP:
		err := cb.output(graph, computeTask, parallel)
		if err != nil {
			return
		}
	case compute.AlgorithmOLTP:
		err := cb.oltpResult(taskID)
		if err != nil {
			return
		}
	}

	computeTask.ComputeWorker.Close()
	computeTask.Task.SetState(structure.TaskStateComplete)
	req := pb.ComputeTaskStatusReq{
		TaskId:     taskID,
		State:      string(structure.TaskStateComplete),
		Step:       computeTask.Step,
		WorkerName: ServiceWorker.WorkerName,
	}
	_, err := ServiceWorker.MasterClient.ComputeTaskStatus(context.Background(), &req)
	if err != nil {
		logrus.Errorf("ComputeTaskStatus err:%v", err)
		cb.SetStatusError(taskID, fmt.Sprintf("ComputeTaskStatus err:%v", err))
	}

	cb.endTask(computeTask.Task.ID)
	common.PrometheusMetrics.TaskRunningCnt.WithLabelValues(computeTask.Task.Type).Dec()
	logrus.Infof("RunOutput completed")
}

func (cb *ComputeBl) output(graph *structure.VermeerGraph, computeTask *compute.ComputerTask, parallel int) error {
	wIdx := graph.GetSelfIndex()
	worker := graph.GetSelfWorker()
	outputType := options.GetString(computeTask.Task.Params, "output.type")
	if outputType == graphio.LoadTypeHugegraph {
		valueType, ok := graph.Data.VertexProperty.GetValueType("label")
		if !ok {
			logrus.Errorf("Hugegraph vertex property label not exist")
			cb.SetStatusError(computeTask.Task.ID, "Hugegraph vertex property label not exist")
			return fmt.Errorf("hugegraph vertex property label not exist")
		}
		if valueType != structure.ValueTypeString {
			logrus.Errorf("Hugegraph vertex property label not string, get type: %v", valueType)
			cb.SetStatusError(computeTask.Task.ID, "Hugegraph vertex property label not string")
			return fmt.Errorf("hugegraph vertex property label not string")
		}

		pdAddr, err := common.FindValidPD(options.GetSliceString(computeTask.Task.Params, "output.hg_pd_peers"))
		if err != nil {
			logrus.Errorf("find valid pd error:%v", err)
			cb.SetStatusError(computeTask.Task.ID, fmt.Sprintf("find valid pd error:%v", err))
			return fmt.Errorf("find valid pd error:%w", err)
		}

		serverAddr, err := common.FindServerAddr(pdAddr,
			options.GetString(computeTask.Task.Params, "output.hugegraph_name"),
			options.GetString(computeTask.Task.Params, "output.hugegraph_username"),
			options.GetString(computeTask.Task.Params, "output.hugegraph_password"))
		if err != nil {
			logrus.Errorf("find server address error:%v", err)
			cb.SetStatusError(computeTask.Task.ID, fmt.Sprintf("find server address error:%v", err))
			return fmt.Errorf("find server address error:%w", err)
		}
		computeTask.Task.Params["output.hugegraph_server"] = serverAddr
	}

	if options.GetInt(computeTask.Task.Params, "output.need_statistics") == 1 {
		cb.statistics(computeTask, graph)
	}

	var uploadVertexValues [][]*pb.VertexValue
	needQuery := options.GetInt(computeTask.Task.Params, "output.need_query") == 1
	if needQuery {
		uploadVertexValues = make([][]*pb.VertexValue, parallel)
		for i := range uploadVertexValues {
			uploadVertexValues[i] = make([]*pb.VertexValue, 0, graph.VertexCount/int64(parallel))
		}
	}

	useOutputFilter := false
	outputExprStr := options.GetString(computeTask.Task.Params, "output.filter_expr")
	filteroutputProps := options.GetSliceString(computeTask.Task.Params, "output.filter_properties")
	vertexFilters := make([]*compute.VertexFilter, parallel)
	if outputExprStr != "" && len(filteroutputProps) > 0 {
		useOutputFilter = true
		for i := range vertexFilters {
			vertexFilters[i] = &compute.VertexFilter{}
			err := vertexFilters[i].Init(outputExprStr, filteroutputProps, graph.Data.VertexProperty)
			if err != nil {
				logrus.Errorf("output filter init error:%v", err)
				useOutputFilter = false
			}
		}
	}

	part := int(worker.VertexCount)/parallel + 1
	cId := int(worker.VertIdStart)
	wg := sync.WaitGroup{}
	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go func(partId int, bId int, eId int) {
			defer func() {
				if r := recover(); r != nil {
					cb.SetStatusError(computeTask.Task.ID, fmt.Sprintf("RunOutput panic recover panic:%v, stack message: %s",
						r, common.GetCurrentGoroutineStack()))
					logrus.Errorf("RunOutput panic recover taskID:%v, pID:%v, panic:%v, stack message: %s",
						computeTask.Task.ID, partId, r, common.GetCurrentGoroutineStack())
				}
			}()
			defer wg.Done()
			if eId > int(worker.VertIdStart+worker.VertexCount) {
				eId = int(worker.VertIdStart + worker.VertexCount)
			}
			writer := graphio.MakeWriter(outputType)
			writerInitInfo := graphio.WriterInitInfo{
				Params: computeTask.Task.Params,
				PartID: wIdx*parallel + partId,
				MaxID:  len(graph.Workers) * parallel,
				Mode:   graphio.WriteModeVertexValue,
			}
			if outputType == graphio.LoadTypeHugegraph {
				writerInitInfo.OutputType = computeTask.ComputeWorker.OutputValueType()
				writerInitInfo.HgVertexSchema = graph.Data.VertexPropertySchema
			}
			err := writer.Init(writerInitInfo)

			if err != nil {
				logrus.Errorf("writer init error:%v", err)
				cb.SetStatusError(computeTask.Task.ID, fmt.Sprintf("writer init error:%v", err))
				return
			}
			defer writer.Close()
			for vId := bId; vId < eId; vId++ {
				if useOutputFilter && !vertexFilters[partId].Filter(uint32(vId)) {
					continue
				}
				writeVertexInfo := graphio.WriteVertexValue{
					VertexID: graph.Data.Vertex.GetVertex(uint32(vId)).ID,
					Value:    computeTask.ComputeWorker.VertexValue(uint32(vId)),
				}
				if needQuery {
					uploadVertexValues[partId] = append(uploadVertexValues[partId], &pb.VertexValue{
						ID:    writeVertexInfo.VertexID,
						Value: writeVertexInfo.Value.ToString(),
					})
				}
				if outputType == graphio.LoadTypeHugegraph {
					writeVertexInfo.HgLabel = string(graph.Data.VertexProperty.GetStringValue("label", uint32(vId)))
				}
				writer.WriteVertex(writeVertexInfo)
			}
		}(i, cId+i*part, cId+((i+1)*part))
	}
	wg.Wait()

	cb.dealQuery(needQuery, uploadVertexValues, computeTask.Task.ID)
	return nil
}

func (cb *ComputeBl) statistics(computeTask *compute.ComputerTask, graph *structure.VermeerGraph) {
	worker := graph.GetSelfWorker()
	mode := compute.StatisticsType(options.GetString(computeTask.Task.Params, "output.statistics_mode"))
	maker := AlgorithmMgr.GetMaker(computeTask.Algorithm)
	statistics := maker.SupportedStatistics()
	if mode == "" {
		for statisticsType := range statistics {
			mode = statisticsType
			break
		}
	}
	if _, ok := statistics[mode]; !ok {
		sKeys := make([]compute.StatisticsType, 0, len(statistics))
		for statisticsType := range statistics {
			sKeys = append(sKeys, statisticsType)
		}
		logrus.Errorf("algorithm %v not support statistics:%v. The options available are: %v ", computeTask.Algorithm, mode, sKeys)
		cb.SetStatusError(computeTask.Task.ID,
			fmt.Sprintf("algorithm %v not support statistics:%v. The options available are: %v ", computeTask.Algorithm, mode, sKeys))
		return
	}
	statisticsWorker := compute.StatisticsWorkerMaker(mode)
	sContext := statisticsWorker.MakeContext()
	sContext.Graph = graph.Data
	statisticsWorker.Init(computeTask.Task.Params)
	for vID := worker.VertIdStart; vID < worker.VertIdStart+worker.VertexCount; vID++ {
		statisticsWorker.Collect(vID, computeTask.ComputeWorker.VertexValue(vID))
	}
	if statisticsWorker.NeedCollectAll() {
		for vID := uint32(0); vID < uint32(graph.VertexCount); vID++ {
			statisticsWorker.CollectAll(vID, computeTask.ComputeWorker.VertexValue(vID))
		}
	}
	output := statisticsWorker.Output()
	_, err := ServiceWorker.MasterClient.UploadStatistics(context.Background(), &pb.UploadStatisticsReq{
		TaskId:     computeTask.Task.ID,
		Statistics: output,
	})
	if err != nil {
		logrus.Errorf("upload statistics err:%v", err)
		cb.SetStatusError(computeTask.Task.ID, fmt.Sprintf("upload statistics err:%v", err))
	}
}

func (cb *ComputeBl) dealQuery(needQuery bool, uploadVertexValues [][]*pb.VertexValue, taskID int32) {
	if needQuery {
		var limit = 2 * 1024 * 1024 * 1024
		for i := range uploadVertexValues {
			uploadVertexReq := &pb.UploadVertexValueReq{
				TaskId:       taskID,
				VertexValues: make([]*pb.VertexValue, 0, 10000),
			}
			size := 0
			for idx := 0; idx < len(uploadVertexValues[i]); idx++ {
				uploadVertexReq.VertexValues = append(uploadVertexReq.VertexValues, uploadVertexValues[i][idx])
				size += len(uploadVertexValues[i][idx].ID) + len(uploadVertexValues[i][idx].Value)
				if size >= limit {
					_, err := ServiceWorker.MasterClient.UploadVertexValue(context.Background(), uploadVertexReq)
					if err != nil {
						logrus.Errorf("upload vertex value err:%v", err)
						cb.SetStatusError(taskID, fmt.Sprintf("upload vertex value err:%v", err))
					}
					uploadVertexReq = &pb.UploadVertexValueReq{
						TaskId:       taskID,
						VertexValues: make([]*pb.VertexValue, 0, 10000),
					}
					size = 0
				}
			}
			_, err := ServiceWorker.MasterClient.UploadVertexValue(context.Background(), uploadVertexReq)
			if err != nil {
				logrus.Errorf("upload vertex value err:%v", err)
				cb.SetStatusError(taskID, fmt.Sprintf("upload vertex value err:%v", err))
			}
		}
	}
}

func (cb *ComputeBl) oltpResult(taskID int32) error {
	defer func() {
		if r := recover(); r != nil {
			cb.SetStatusError(taskID, fmt.Sprintf("RunOutput panic recover panic:%v, stack message: %s", r,
				common.GetCurrentGoroutineStack()))
			logrus.Errorf("RunOutput panic recover taskID:%v, panic:%v, stack message: %s", taskID, r,
				common.GetCurrentGoroutineStack())
		}
	}()
	computeTask := ComputeTaskMgr.GetTask(taskID)
	computeTask.ComputeWorker.Context().Output = true

	bytes := computeTask.ComputeWorker.Output()

	_, err := ServiceWorker.MasterClient.UploadTPResult(context.Background(), &pb.UploadTPResultReq{
		TaskId: taskID,
		Result: bytes,
	})
	if err != nil {
		logrus.Errorf("upload tp result err:%v", err)
		cb.SetStatusError(computeTask.Task.ID, fmt.Sprintf("upload tp result err:%v", err))
		return fmt.Errorf("upload tp result err:%w", err)
	}
	return nil
}

func (cb *ComputeBl) SetStatusError(taskId int32, msg string) {
	computeTask := ComputeTaskMgr.GetTask(taskId)
	if computeTask == nil {
		return
	}
	computeTask.Task.State = structure.TaskStateError
	logrus.Errorf("compute task error: %d", taskId)
	req := pb.ComputeTaskStatusReq{
		WorkerName: ServiceWorker.WorkerName,
		TaskId:     taskId,
		State:      string(structure.TaskStateError),
		ErrorMsg:   msg,
	}
	_, err := ServiceWorker.MasterClient.ComputeTaskStatus(context.Background(), &req)
	if err != nil {
		logrus.Errorf("LoadTaskStatus error: %s", err)
	}
	time.AfterFunc(1*time.Minute, func() { cb.endTask(computeTask.Task.ID) })
	common.PrometheusMetrics.TaskRunningCnt.WithLabelValues(computeTask.Task.Type).Dec()
}

func (cb *ComputeBl) endTask(taskID int32) {
	computeTask := ComputeTaskMgr.GetTask(taskID)
	if computeTask != nil {
		computeTask.FreeMemory()
		ComputeTaskMgr.DeleteTask(computeTask.Task.ID)
	}
}

func (cb *ComputeBl) CheckAction(computeTask *compute.ComputerTask) (isContinue bool) {
	if computeTask == nil {
		return true
	}
	switch atomic.LoadInt32(&computeTask.Task.Action) {
	case structure.ActionDoNoting:

	case structure.ActionCancelTask:
		cb.cancelAction(computeTask)
		return false
	case structure.ActionPauseTask:
		return cb.pauseAction(computeTask)
	default:
		logrus.Errorf("unknown action %d", computeTask.Task.Action)
	}

	return true
}

func (cb *ComputeBl) cancelAction(computeTask *compute.ComputerTask) {
	computeTask.CancelOnce.Do(func() {
		computeTask.Task.SetState(structure.TaskStateCanceled)
		common.PrometheusMetrics.TaskRunningCnt.WithLabelValues(computeTask.Task.Type).Dec()
		time.AfterFunc(1*time.Minute, func() { cb.endTask(computeTask.Task.ID) })
	})
}

func (cb *ComputeBl) pauseAction(computeTask *compute.ComputerTask) bool {
	task := computeTask.Task
	for {
		switch atomic.LoadInt32(&task.Action) {
		case structure.ActionCancelTask:
			cb.cancelAction(computeTask)
			return false
		case structure.ActionResumeTask:
			return true
		default:
			time.Sleep(10 * time.Second)
		}
	}
}
