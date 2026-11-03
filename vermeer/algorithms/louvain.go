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

package algorithms

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"vermeer/apps/common"
	"vermeer/apps/compute"
	"vermeer/apps/options"
	"vermeer/apps/serialize"

	"github.com/sirupsen/logrus"
)

func init() {
	Algorithms = append(Algorithms, new(LouvainMaker))
}

type LouvainMaker struct {
	compute.AlgorithmMakerBase
}

func (lv *LouvainMaker) CreateWorkerComputer() compute.WorkerComputer {
	return &LouvainWorker{}
}

func (lv *LouvainMaker) CreateMasterComputer() compute.MasterComputer {
	return &LouvainMaster{}
}

func (lv *LouvainMaker) Name() string {
	return "louvain"
}

func (lv *LouvainMaker) DataNeeded() []string {
	return []string{compute.UseOutEdge}
}

func (lv *LouvainMaker) SupportedStatistics() map[compute.StatisticsType]struct{} {
	return map[compute.StatisticsType]struct{}{compute.StatisticsTypeCount: {}, compute.StatisticsTypeModularity: {}, compute.StatisticsTypeSketchCount: {}}
}

// louvainNode 图中合并的每个节点
type louvainNode struct {
	//node 本节点内的所有顶点
	vertex []serialize.SUint32
	//本节点的所有邻居节点
	neighbors map[serialize.SUint32]float64
	//本节点的所有度之和
	kI float64
	//本节点内部的连接度权重之和
	KIn float64
	//本节点当前所在的社区id
	commID serialize.SUint32
	//once
	once int32
}

type community struct {
	//社区包含的点
	node map[serialize.SUint32]struct{}
	//社区的总度数
	sigmaTot float64
}

type LouvainWorker struct {
	compute.WorkerComputerBase
	//nodeID
	nodeID []serialize.SUint32
	//node struct map
	nodes map[serialize.SUint32]*louvainNode
	//louvain step聚合后社区相关信息
	communities map[serialize.SUint32]*community
	//所有点的邻居节点
	neighborEdges []serialize.SliceUint32
	//最终的节点变化
	node2comm []map[serialize.SUint32]serialize.SUint32
	//误分节点导入至空社区
	emptyComm serialize.SUint32
	//neighbor KIin
	//neighborCommKVInInPID []map[serialize.SUint32]float64
	//total edge nums,带权重
	edgeNums float64
	//resolution :  double, optional
	//Will change the size of the communities, default to 1.
	//represents the time described in
	//"Laplacian Dynamics and Multiscale Modular Structure in Networks",
	//R. Lambiotte, J.-C. Delvenne, M. Barahona
	resolution float64
	//parallel
	parallel int
	//第一阶段，以顶点为单位
	firstStep       bool
	firstStepKI     []float64
	firstStepCommID []serialize.SUint32
}

func (lw *LouvainWorker) VertexValue(i uint32) serialize.MarshalAble {
	if lw.WContext.Step == 1 {
		return &lw.neighborEdges[i]
	}
	if lw.WContext.Output {
		return &lw.nodeID[i]
	}
	nilValue := serialize.SInt32(0)
	return &nilValue
}

// ValidateOutput rejects early termination before community IDs are available.
func (lw *LouvainWorker) ValidateOutput() error {
	if lw.firstStep {
		return fmt.Errorf("louvain did not finish its first pass; increase compute.max_step")
	}
	return nil
}

func (lw *LouvainWorker) Init() error {
	lw.communities = make(map[serialize.SUint32]*community, lw.WContext.GraphData.Vertex.TotalVertexCount())

	lw.neighborEdges = make([]serialize.SliceUint32, lw.WContext.GraphData.Vertex.TotalVertexCount())

	lw.resolution = options.GetFloat(lw.WContext.Params, "louvain.resolution")
	lw.parallel = lw.WContext.Parallel
	if lw.parallel <= 0 {
		logrus.Infof("parallel value must be larger than 0, get: %v, set to defalut value :1", lw.parallel)
		lw.parallel = 1
	}
	lw.node2comm = make([]map[serialize.SUint32]serialize.SUint32, lw.parallel)
	for i := range lw.node2comm {
		lw.node2comm[i] = make(map[serialize.SUint32]serialize.SUint32, lw.WContext.GraphData.Vertex.TotalVertexCount()/uint32(lw.parallel))
	}
	//lw.neighborCommKVInInPID = make([]map[serialize.SUint32]float64, lw.parallel)
	lw.WContext.CreateValue("change_node", compute.ValueTypeSliceUint32, compute.CValueActionAggregate)
	lw.WContext.SetValue("change_node", serialize.SliceUint32{})
	lw.WContext.CreateValue("change_comm", compute.ValueTypeSliceUint32, compute.CValueActionAggregate)
	lw.WContext.SetValue("change_comm", serialize.SliceUint32{})
	lw.WContext.CreateValue("mod_value", compute.ValueTypeFloat32, compute.CValueActionAggregate)
	lw.WContext.SetValue("mod_value", serialize.SFloat32(0))

	lw.WContext.CreateValue("update", compute.ValueTypeInt32, compute.CValueActionAggregate)
	lw.WContext.SetValue("update", serialize.SInt32(0))

	lw.emptyComm = serialize.SUint32(lw.WContext.GraphData.Vertex.TotalVertexCount() + 1)
	lw.firstStep = true
	return nil
}

func (lw *LouvainWorker) BeforeStep() {
	if lw.WContext.Step == 2 {
		lw.firstStepKI = make([]float64, lw.WContext.GraphData.Vertex.TotalVertexCount())
		lw.firstStepCommID = make([]serialize.SUint32, lw.WContext.GraphData.Vertex.TotalVertexCount())
		for vertexID, edges := range lw.neighborEdges {
			if len(edges) == 0 {
				continue
			}
			lw.edgeNums += float64(len(edges))
			lw.communities[serialize.SUint32(vertexID)] = &community{node: map[serialize.SUint32]struct{}{serialize.SUint32(vertexID): {}}}
			lw.communities[serialize.SUint32(vertexID)].sigmaTot = float64(len(edges))
			lw.firstStepKI[vertexID] = float64(len(edges))
			lw.firstStepCommID[vertexID] = serialize.SUint32(vertexID)
		}
		lw.resolution /= lw.edgeNums
		lw.WContext.SetValue("mod_value", serialize.SFloat32(-1))
	} else if lw.WContext.Step > 2 {
		changeNode := lw.WContext.GetValue("change_node").(serialize.SliceUint32)
		changeComm := lw.WContext.GetValue("change_comm").(serialize.SliceUint32)
		//changes := make(map[serialize.SUint32]serialize.SUint32)
		currCommIDs := make(map[serialize.SUint32]struct{})
		moveToEmpty := make([]serialize.SUint32, 0)
		for i, node := range changeNode {
			if changeComm[i] == lw.emptyComm {
				moveToEmpty = append(moveToEmpty, node)
				continue
			}
			var currCommID serialize.SUint32
			var ki float64
			if lw.firstStep {
				currCommID = lw.firstStepCommID[node]
				ki = lw.firstStepKI[node]
				lw.firstStepCommID[node] = changeComm[i]
			} else {
				currCommID = lw.nodes[node].commID
				ki = lw.nodes[node].kI
				lw.nodes[node].commID = changeComm[i]
			}
			currCommIDs[currCommID] = struct{}{}
			delete(lw.communities[currCommID].node, node)
			lw.communities[currCommID].sigmaTot -= ki
			lw.communities[changeComm[i]].node[node] = struct{}{}
			lw.communities[changeComm[i]].sigmaTot += ki
		}
		if len(moveToEmpty) > 0 {
			logrus.Infof("move to empty node len:%v", len(moveToEmpty))
			emptyComms := make([]serialize.SUint32, len(moveToEmpty))
			idx := 0
			for i := 0; i < int(lw.WContext.GraphData.Vertex.TotalVertexCount()); i++ {
				if idx == len(moveToEmpty) {
					break
				}
				if comm, ok := lw.communities[serialize.SUint32(i)]; ok {
					if len(comm.node) == 0 {
						emptyComms[idx] = serialize.SUint32(i)
						idx++
					}
				}
			}
			MoveOutComm := make(map[serialize.SUint32]int)
			for _, node := range moveToEmpty {
				var currCommID serialize.SUint32
				if lw.firstStep {
					currCommID = lw.firstStepCommID[node]
				} else {
					currCommID = lw.nodes[node].commID
				}
				currCommIDs[currCommID] = struct{}{}
				MoveOutComm[currCommID] += 1
			}
			alreadyMoveOutComm := make(map[serialize.SUint32]int, len(MoveOutComm))
			for i, node := range moveToEmpty {
				var ki float64
				var currCommID serialize.SUint32
				if lw.firstStep {
					currCommID = lw.firstStepCommID[node]
				} else {
					currCommID = lw.nodes[node].commID
				}
				alreadyMoveOutComm[currCommID] += 1
				if alreadyMoveOutComm[currCommID] > MoveOutComm[currCommID]/2 && MoveOutComm[currCommID] > 1 {
					continue
				}
				if lw.firstStep {
					ki = lw.firstStepKI[node]
					lw.firstStepCommID[node] = emptyComms[i]
				} else {
					ki = lw.nodes[node].kI
					lw.nodes[node].commID = emptyComms[i]
				}
				delete(lw.communities[currCommID].node, node)
				lw.communities[currCommID].sigmaTot -= ki
				lw.communities[emptyComms[i]].node[node] = struct{}{}
				lw.communities[emptyComms[i]].sigmaTot += ki
			}
		}
		update := lw.WContext.GetValue("update").(serialize.SInt32)
		if update > 0 {
			lw.deleteEmptyComm()
			if lw.firstStep {
				lw.firstStep = false
				//初始化node
				lw.initLouvainNode()
				//free memory
				lw.firstStepCommID = nil
				lw.firstStepKI = nil
				lw.neighborEdges = nil
			} else {
				//生成新图
				lw.genNewGraph()
			}
			lw.WContext.SetValue("mod_value", serialize.SFloat32(lw.calModularity()))
		} else {
			lw.optimizeMem(currCommIDs)
			lw.WContext.SetValue("mod_value", serialize.SFloat32(-1))
		}
		lw.node2comm = make([]map[serialize.SUint32]serialize.SUint32, lw.parallel)
		for i := range lw.node2comm {
			lw.node2comm[i] = make(map[serialize.SUint32]serialize.SUint32, len(changeNode)/lw.parallel)
		}
		for nodeID := range lw.nodes {
			lw.nodes[nodeID].once = 0
		}
	}
	logrus.Infof("communities num:%v", len(lw.communities))
}

func (lw *LouvainWorker) Compute(vertexID uint32, pID int) {
	//step 1:同步所有顶点的邻边
	vertID := vertexID - lw.WContext.GraphData.VertIDStart
	if len(lw.WContext.GraphData.Edges.GetInEdges(vertID))+len(lw.WContext.GraphData.Edges.GetOutEdges(vertID)) == 0 {
		return
	}
	if lw.WContext.Step == 1 {
		lw.neighborEdges[vertexID] = make([]serialize.SUint32, 0, len(lw.WContext.GraphData.Edges.GetInEdges(vertID)))
		tempMap := make(map[serialize.SUint32]struct{})
		tempMap[serialize.SUint32(vertexID)] = struct{}{}
		for _, edge := range lw.WContext.GraphData.Edges.GetInEdges(vertID) {
			if _, ok := tempMap[edge]; !ok {
				lw.neighborEdges[vertexID] = append(lw.neighborEdges[vertexID], edge)
				tempMap[edge] = struct{}{}
			}
		}
		for _, edge := range lw.WContext.GraphData.Edges.GetOutEdges(vertID) {
			if _, ok := tempMap[edge]; !ok {
				lw.neighborEdges[vertexID] = append(lw.neighborEdges[vertexID], edge)
				tempMap[edge] = struct{}{}
			}
		}
	} else {
		if lw.firstStep {
			//以vertex为基本单元计算
			currCommID := lw.firstStepCommID[vertexID]
			kI := lw.firstStepKI[vertexID]

			//neighborCommKIin  计算neighbor社区的KIin
			neighborCommKVInInPID := make(map[serialize.SUint32]float64, len(lw.neighborEdges[vertexID]))

			for _, neighbor := range lw.neighborEdges[vertexID] {
				neighborCommID := lw.firstStepCommID[neighbor]
				neighborCommKVInInPID[neighborCommID] += 1
			}

			var maxDeltaQ float64
			targetCommID := currCommID
			for neighborCommID, kVIn := range neighborCommKVInInPID {
				sigmaTot := lw.communities[neighborCommID].sigmaTot
				if currCommID == neighborCommID {
					sigmaTot -= kI
				}
				commDeltaQ := lw.calDeltaQ(kVIn, sigmaTot, kI)
				if commDeltaQ > maxDeltaQ {
					targetCommID = neighborCommID
					maxDeltaQ = commDeltaQ
				}
			}
			if maxDeltaQ == 0 && len(lw.communities[currCommID].node) > 1 {
				lw.node2comm[pID][serialize.SUint32(vertexID)] = lw.emptyComm
			}
			if targetCommID >= currCommID {
				return
			}
			lw.node2comm[pID][serialize.SUint32(vertexID)] = targetCommID
		} else {
			nodeID := lw.nodeID[vertexID]
			if lw.nodes[nodeID] == nil || atomic.LoadInt32(&lw.nodes[nodeID].once) > 0 {
				return
			}
			atomic.AddInt32(&lw.nodes[nodeID].once, 1)
			currCommID := lw.nodes[nodeID].commID
			kI := lw.nodes[nodeID].kI

			//neighborCommKIin  计算neighbor社区的KIin
			neighborCommKVInInPID := make(map[serialize.SUint32]float64, len(lw.nodes[nodeID].neighbors))
			for neighbor, weight := range lw.nodes[nodeID].neighbors {
				neighborCommID := lw.nodes[neighbor].commID
				neighborCommKVInInPID[neighborCommID] += weight
			}

			var maxDeltaQ float64
			targetCommID := currCommID
			for neighborCommID, kVIn := range neighborCommKVInInPID {
				sigmaTot := lw.communities[neighborCommID].sigmaTot
				if currCommID == neighborCommID {
					sigmaTot -= kI
				}
				commDeltaQ := lw.calDeltaQ(kVIn, sigmaTot, kI)
				if commDeltaQ > maxDeltaQ {
					targetCommID = neighborCommID
					maxDeltaQ = commDeltaQ
				}
			}

			if maxDeltaQ == 0 && len(lw.communities[currCommID].node) > 1 {
				lw.node2comm[pID][nodeID] = lw.emptyComm
			}
			if targetCommID >= currCommID {
				return
			}
			lw.node2comm[pID][nodeID] = targetCommID
		}
	}
}

func (lw *LouvainWorker) AfterStep() {
	if lw.WContext.Step >= 2 {
		changeNode := make([]serialize.SUint32, 0, len(lw.node2comm))
		changeComm := make([]serialize.SUint32, 0, len(lw.node2comm))
		for _, node2comm := range lw.node2comm {
			for node, comm := range node2comm {
				changeNode = append(changeNode, node)
				changeComm = append(changeComm, comm)
			}
		}
		//logrus.Infof("changenode:%v,changecomm:%v", changeNode, changeComm)
		lw.WContext.SetValue("change_node", serialize.SliceUint32(changeNode))
		lw.WContext.SetValue("change_comm", serialize.SliceUint32(changeComm))
	}
}

func (lw *LouvainWorker) OutputValueType() string {
	return common.HgValueTypeInt
}

func (lw *LouvainWorker) optimizeMem(currCommIDs map[serialize.SUint32]struct{}) {
	//优化内存
	commIDs := make([]serialize.SUint32, 0, len(currCommIDs))
	for commID := range currCommIDs {
		commIDs = append(commIDs, commID)
	}
	partCnt := len(commIDs)/lw.parallel + 1
	wg := &sync.WaitGroup{}
	for i := 0; i < lw.parallel; i++ {
		wg.Add(1)
		go func(pID int) {
			defer wg.Done()
			bIdx := partCnt * pID
			if bIdx > len(commIDs) {
				return
			}
			eIdx := bIdx + partCnt
			if eIdx > len(commIDs) {
				eIdx = len(commIDs)
			}
			for i := bIdx; i < eIdx; i++ {
				commID := commIDs[i]
				newNodes := make(map[serialize.SUint32]struct{}, len(lw.communities[commID].node))
				for node := range lw.communities[commID].node {
					newNodes[node] = struct{}{}
				}
				lw.communities[commID].node = newNodes
			}
		}(i)
	}
	wg.Wait()
	//for commID := range currCommIDs {
	//	newNodes := make(map[serialize.SUint32]struct{}, len(lw.communities[commID].node))
	//	for node := range lw.communities[commID].node {
	//		newNodes[node] = struct{}{}
	//	}
	//	lw.communities[commID].node = newNodes
	//}
}

func (lw *LouvainWorker) deleteEmptyComm() {
	//删除空的社区
	for commID, comm := range lw.communities {
		if len(comm.node) == 0 {
			delete(lw.communities, commID)
		}
	}
	newComm := make(map[serialize.SUint32]*community, len(lw.communities))
	for commID, comm := range lw.communities {
		newComm[commID] = comm
	}
	lw.communities = newComm
}

func (lw *LouvainWorker) calDeltaQ(kVIn, sigmaTot, kI float64) float64 {
	//DeltaQ = k_v_in - tot * k_v / m
	//各元素物理意义：
	//k_v_in: 当前点指向目标点所在社区的边权值之和
	//tot: 目标点所在社区内边外边权重之和（如果当前点和目标点处在同一个社区，去要减掉一个k_v）
	//k_v: 当前点内外度之和
	//m: 全图边权重之和,已处理在resolution之内
	return kVIn - lw.resolution*sigmaTot*kI
}

func (lw *LouvainWorker) calModularity() float64 {
	//模块度计算，可以实现并行计算
	//.. math::
	//Q = \sum_{c=1}^{n}
	//\left[ \frac{L_c}{m} - \gamma\left( \frac{k_c}{2m} \right) ^2 \right]
	//
	//where the sum iterates over all communities $c$, $m$ is the number of edges,
	//$L_c$ is the number of intra-community links for community $c$,
	//$k_c$ is the sum of degrees of the nodes in community $c$,
	//and $\gamma$ is the resolution parameter.
	var mod float64
	commIDs := make([]serialize.SUint32, 0, len(lw.communities))
	for commID := range lw.communities {
		commIDs = append(commIDs, commID)
	}
	wg := &sync.WaitGroup{}
	locker := &sync.Mutex{}
	partCnt := len(commIDs)/lw.parallel + 1
	for i := 0; i < lw.parallel; i++ {
		wg.Add(1)
		go func(pID int) {
			defer wg.Done()
			bIdx := partCnt * pID
			if bIdx > len(commIDs) {
				return
			}
			eIdx := bIdx + partCnt
			if eIdx > len(commIDs) {
				eIdx = len(commIDs)
			}
			var modInPID float64
			for i := bIdx; i < eIdx; i++ {
				commID := commIDs[i]
				if int(commID)%lw.WContext.Workers == lw.WContext.WorkerIdx {
					comm := lw.communities[commID]
					var commInDegree float64
					for nodeID := range comm.node {
						commInDegree += lw.nodes[nodeID].KIn
						for neighborID, weight := range lw.nodes[nodeID].neighbors {
							if lw.nodes[neighborID].commID == commID {
								commInDegree += weight
							}
						}
					}
					modInPID += commInDegree/lw.edgeNums - (comm.sigmaTot/lw.edgeNums)*comm.sigmaTot*lw.resolution
				}
			}
			locker.Lock()
			mod += modInPID
			locker.Unlock()
		}(i)
	}
	wg.Wait()
	return mod
}

func (lw *LouvainWorker) initLouvainNode() {
	//gen a new graph for new step
	//可以并行
	lw.nodes = make(map[serialize.SUint32]*louvainNode, len(lw.communities))
	lw.nodeID = make([]serialize.SUint32, lw.WContext.GraphData.Vertex.TotalVertexCount())
	for i := range lw.nodeID {
		lw.nodeID[i] = serialize.SUint32(i)
	}
	locker := &sync.Mutex{}
	commIDs := make([]serialize.SUint32, 0, len(lw.communities))
	for commID := range lw.communities {
		commIDs = append(commIDs, commID)
	}
	wg := &sync.WaitGroup{}
	partCnt := len(commIDs)/lw.parallel + 1
	for i := 0; i < lw.parallel; i++ {
		wg.Add(1)
		go func(pID int) {
			defer wg.Done()
			bIdx := partCnt * pID
			if bIdx > len(commIDs) {
				return
			}
			eIdx := bIdx + partCnt
			if eIdx > len(commIDs) {
				eIdx = len(commIDs)
			}
			newNodesInPID := make(map[serialize.SUint32]*louvainNode, len(lw.communities)/lw.parallel)
			for i := bIdx; i < eIdx; i++ {
				commID := commIDs[i]
				comm := lw.communities[commID]
				newNodesInPID[commID] = &louvainNode{
					vertex:    make([]serialize.SUint32, 0, len(comm.node)),
					neighbors: make(map[serialize.SUint32]float64),
					kI:        comm.sigmaTot,
					commID:    commID,
				}
				for vertex := range comm.node {
					newNodesInPID[commID].vertex = append(newNodesInPID[commID].vertex, vertex)
					for _, neighbor := range lw.neighborEdges[vertex] {
						if _, ok := comm.node[neighbor]; ok {
							newNodesInPID[commID].KIn += 1
							continue
						}
						newNodesInPID[commID].neighbors[lw.firstStepCommID[neighbor]] += 1
					}
					lw.nodeID[vertex] = commID
				}
				lw.communities[commID].node = make(map[serialize.SUint32]struct{})
				lw.communities[commID].node[commID] = struct{}{}
			}
			locker.Lock()
			for i := bIdx; i < eIdx; i++ {
				commID := commIDs[i]
				lw.nodes[commID] = newNodesInPID[commID]
			}
			locker.Unlock()
		}(i)
	}
	wg.Wait()
}

func (lw *LouvainWorker) genNewGraph() {
	//gen a new graph for new step
	//可以并行
	newNodes := make(map[serialize.SUint32]*louvainNode, len(lw.communities))
	locker := &sync.Mutex{}
	commIDs := make([]serialize.SUint32, 0, len(lw.communities))
	for commID := range lw.communities {
		commIDs = append(commIDs, commID)
	}
	wg := &sync.WaitGroup{}
	partCnt := len(commIDs)/lw.parallel + 1
	for i := 0; i < lw.parallel; i++ {
		wg.Add(1)
		go func(pID int) {
			defer wg.Done()
			bIdx := partCnt * pID
			if bIdx > len(commIDs) {
				return
			}
			eIdx := bIdx + partCnt
			if eIdx > len(commIDs) {
				eIdx = len(commIDs)
			}
			newNodesInPID := make(map[serialize.SUint32]*louvainNode, len(lw.communities))
			for i := bIdx; i < eIdx; i++ {
				commID := commIDs[i]
				comm := lw.communities[commID]
				//合并comm.node中的所有node到newNodes
				newNodesInPID[commID] = &louvainNode{
					commID:    commID,
					neighbors: make(map[serialize.SUint32]float64),
					vertex:    make([]serialize.SUint32, 0)}
				for oldNodeID := range comm.node {
					newNodesInPID[commID].vertex = append(newNodesInPID[commID].vertex, lw.nodes[oldNodeID].vertex...)
					newNodesInPID[commID].kI += lw.nodes[oldNodeID].kI
					newNodesInPID[commID].KIn += lw.nodes[oldNodeID].KIn
					for neighborID, weight := range lw.nodes[oldNodeID].neighbors {
						if lw.nodes[neighborID].commID == commID {
							newNodesInPID[commID].KIn += weight
							continue
						}
						newNodesInPID[commID].neighbors[lw.nodes[neighborID].commID] += weight
					}
				}
				for _, vertexID := range newNodesInPID[commID].vertex {
					lw.nodeID[vertexID] = commID
				}
				lw.communities[commID].node = make(map[serialize.SUint32]struct{})
				lw.communities[commID].node[commID] = struct{}{}
			}
			locker.Lock()
			for i := bIdx; i < eIdx; i++ {
				commID := commIDs[i]
				newNodes[commID] = newNodesInPID[commID]
			}
			locker.Unlock()
		}(i)
	}
	wg.Wait()
	lw.nodes = newNodes
}

type LouvainMaster struct {
	compute.MasterComputerBase
	//阈值，总模块度值的变化是否小于阈值判断是否退出算法。
	threshold float64
	//前一个收敛完的迭代得到的模块度
	prevModValue serialize.SFloat32
	louvainStep  int
	maxStep      int
}

func (lm *LouvainMaster) Init() error {
	lm.threshold = options.GetFloat(lm.MContext.Params, "louvain.threshold")
	lm.maxStep = options.GetInt(lm.MContext.Params, "louvain.step")
	lm.prevModValue = math.MinInt32
	lm.louvainStep = 1
	return nil
}

func (lm *LouvainMaster) Compute() bool {
	//对比模块度变化，小于阈值则提前退出
	if lm.MContext.Step >= 2 {
		changeNode := lm.MContext.GetValue("change_node").(serialize.SliceUint32)
		changeComm := lm.MContext.GetValue("change_comm").(serialize.SliceUint32)
		newNodes := make([]serialize.SUint32, 0, len(changeNode))
		newComms := make([]serialize.SUint32, 0, len(changeComm))
		nodes := make(map[serialize.SUint32]struct{}, len(changeNode))
		for i, node := range changeNode {
			if _, ok := nodes[node]; ok {
				continue
			}
			nodes[node] = struct{}{}
			newNodes = append(newNodes, node)
			newComms = append(newComms, changeComm[i])
		}
		logrus.Infof("changes len:%v", len(newNodes))
		lm.MContext.SetValue("change_node", serialize.SliceUint32(newNodes))
		lm.MContext.SetValue("change_comm", serialize.SliceUint32(newComms))
		if len(changeNode) == 0 {
			lm.louvainStep++
			lm.MContext.SetValue("update", serialize.SInt32(1))
		} else {
			lm.MContext.SetValue("update", serialize.SInt32(0))
		}
		//获取总模块度，与之前记录的总模块度相比较，判断是否退出
		modValue := lm.MContext.GetValue("mod_value").(serialize.SFloat32)
		if modValue <= -1 {
			return true
		}
		lm.MContext.SetValue("mod_value", serialize.SFloat32(0))
		logrus.Infof("Step:%v, Modularity:%v", lm.louvainStep, modValue)
		if float64(modValue-lm.prevModValue) <= lm.threshold || lm.louvainStep == lm.maxStep {
			lm.prevModValue = modValue
			return false
		} else {
			lm.prevModValue = modValue
		}
	}

	return true
}

func (lm *LouvainMaster) Statistics() map[string]any {
	return map[string]any{
		"modularity_in_louvain": lm.prevModValue,
	}
}
