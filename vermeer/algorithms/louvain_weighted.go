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
	"math/rand"
	"sync"
	"sync/atomic"
	"vermeer/apps/common"
	"vermeer/apps/compute"
	"vermeer/apps/options"
	"vermeer/apps/serialize"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"
)

func init() {
	Algorithms = append(Algorithms, new(LouvainWeightedMaker))
}

type LouvainWeightedMaker struct {
	compute.AlgorithmMakerBase
}

func (lv *LouvainWeightedMaker) CreateWorkerComputer() compute.WorkerComputer {
	return &LouvainWeightedWorker{}
}

func (lv *LouvainWeightedMaker) CreateMasterComputer() compute.MasterComputer {
	return &LouvainWeightedMaster{}
}

func (lv *LouvainWeightedMaker) Name() string {
	return "louvain_weighted"
}

func (lv *LouvainWeightedMaker) DataNeeded() []string {
	return []string{compute.UseOutEdge}
}

func (lv *LouvainWeightedMaker) SupportedStatistics() map[compute.StatisticsType]struct{} {
	return map[compute.StatisticsType]struct{}{compute.StatisticsTypeCount: {}, compute.StatisticsTypeModularity: {}}
}

type LouvainWeightedWorker struct {
	compute.WorkerComputerBase
	//nodeID
	nodeID []serialize.SUint32
	//node struct map
	nodes map[serialize.SUint32]*louvainNode
	//louvain step聚合后社区相关信息
	communities map[serialize.SUint32]*community
	//所有点的邻居节点
	neighborEdges []serialize.MapUint32Float32
	// 临时存储inedge节点
	inEdges []serialize.MapUint32Float32
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
	outputReady     int32

	// property weight
	useProperty  bool
	propertyType structure.ValueType
	edgeProperty string
}

func (lw *LouvainWeightedWorker) VertexValue(i uint32) serialize.MarshalAble {
	if lw.WContext.Step == 1 {
		return &lw.inEdges[i]
	} else if lw.WContext.Step == 2 {
		return &lw.neighborEdges[i]
	}
	if lw.WContext.Output {
		return &lw.nodeID[i]
	}
	nilValue := serialize.SInt32(0)
	return &nilValue
}

// ValidateOutput rejects early termination before community IDs are available.
func (lw *LouvainWeightedWorker) ValidateOutput() error {
	if atomic.LoadInt32(&lw.outputReady) == 0 {
		return fmt.Errorf("louvain_weighted did not finish publishing community IDs; increase compute.max_step")
	}
	return nil
}

func (lw *LouvainWeightedWorker) Init() error {
	lw.communities = make(map[serialize.SUint32]*community, lw.WContext.GraphData.Vertex.TotalVertexCount())

	lw.neighborEdges = make([]serialize.MapUint32Float32, lw.WContext.GraphData.Vertex.TotalVertexCount())
	lw.inEdges = make([]serialize.MapUint32Float32, lw.WContext.GraphData.Vertex.TotalVertexCount())

	lw.resolution = options.GetFloat(lw.WContext.Params, "louvain.resolution")
	if err := validatePositiveFinite("louvain.resolution", lw.resolution); err != nil {
		return err
	}
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
	atomic.StoreInt32(&lw.outputReady, 0)

	// load edge weight
	lw.edgeProperty = options.GetString(lw.WContext.Params, "louvain.edge_weight_property")
	if len(lw.edgeProperty) > 0 {
		vType, ok := lw.WContext.GraphData.InEdgesProperty.GetValueType(lw.edgeProperty)
		if !ok {
			logrus.Errorf("unknown edge weighted property:%v", lw.edgeProperty)
			return fmt.Errorf("unknown edge weighted property:%v", lw.edgeProperty)
		}
		switch vType {
		case structure.ValueTypeInt32, structure.ValueTypeFloat32:
			lw.propertyType = vType
		case structure.ValueTypeString:
			logrus.Errorf("illegal edge weighted property type:%v", lw.edgeProperty)
			return fmt.Errorf("illegal edge weighted property type:%v", lw.edgeProperty)
		}
		lw.useProperty = true
		if err := lw.validateEdgeWeights(); err != nil {
			return err
		}
	}
	return nil
}

func (lw *LouvainWeightedWorker) validateEdgeWeights() error {
	for localID := uint32(0); localID < lw.WContext.GraphData.VertexCount; localID++ {
		inEdges := lw.WContext.GraphData.Edges.GetInEdges(localID)
		for edgeIndex := range inEdges {
			var weight float64
			switch lw.propertyType {
			case structure.ValueTypeInt32:
				weight = float64(lw.WContext.GraphData.InEdgesProperty.GetInt32Value(
					lw.edgeProperty, localID, uint32(edgeIndex)))
			case structure.ValueTypeFloat32:
				weight = float64(lw.WContext.GraphData.InEdgesProperty.GetFloat32Value(
					lw.edgeProperty, localID, uint32(edgeIndex)))
			}
			if err := validateNonNegativeFinite("louvain edge weight", weight); err != nil {
				vertexID := lw.WContext.GraphData.VertIDStart + localID
				return fmt.Errorf("property %q at vertex %d, incoming edge %d: %w",
					lw.edgeProperty, vertexID, edgeIndex, err)
			}
		}
	}
	return nil
}

func (lw *LouvainWeightedWorker) BeforeStep() {
	if lw.WContext.Step == 3 {
		lw.inEdges = nil

		lw.firstStepKI = make([]float64, lw.WContext.GraphData.Vertex.TotalVertexCount())
		lw.firstStepCommID = make([]serialize.SUint32, lw.WContext.GraphData.Vertex.TotalVertexCount())
		for vertexID, edges := range lw.neighborEdges {
			if len(edges) == 0 {
				continue
			}
			var wgtSum float64
			for _, wgt := range edges {
				wgtSum += float64(wgt)
			}
			lw.edgeNums += wgtSum
			lw.communities[serialize.SUint32(vertexID)] = &community{node: map[serialize.SUint32]struct{}{serialize.SUint32(vertexID): {}}}
			lw.communities[serialize.SUint32(vertexID)].sigmaTot = wgtSum
			lw.firstStepKI[vertexID] = wgtSum
			lw.firstStepCommID[vertexID] = serialize.SUint32(vertexID)
		}
		if lw.edgeNums == 0 {
			lw.nodeID = identityCommunityIDs(lw.WContext.GraphData.Vertex.TotalVertexCount())
			lw.nodes = make(map[serialize.SUint32]*louvainNode)
			lw.communities = make(map[serialize.SUint32]*community)
			lw.firstStep = false
			lw.firstStepKI = nil
			lw.firstStepCommID = nil
			lw.neighborEdges = nil
			lw.WContext.SetValue("mod_value", serialize.SFloat32(0))
			atomic.StoreInt32(&lw.outputReady, 1)
			return
		}
		lw.resolution /= lw.edgeNums
		logrus.Infof("edge nums:%v", lw.edgeNums)
		logrus.Infof("resolution:%v", lw.resolution)
		lw.WContext.SetValue("mod_value", serialize.SFloat32(-1))
	} else if lw.WContext.Step > 3 {
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
			atomic.StoreInt32(&lw.outputReady, 0)
			lw.deleteEmptyComm()
			if lw.firstStep {
				//初始化node
				lw.initLouvainNode()
				lw.firstStep = false
				//free memory
				lw.firstStepCommID = nil
				lw.firstStepKI = nil
				lw.neighborEdges = nil
			} else {
				//生成新图
				lw.genNewGraph()
			}
			atomic.StoreInt32(&lw.outputReady, 1)
			lw.WContext.SetValue("mod_value", serialize.SFloat32(lw.calModularity()))
		} else {
			if rand.Float32() < 0.5 {
				lw.optimizeMem(currCommIDs)
			}
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

func (lw *LouvainWeightedWorker) Compute(vertexID uint32, pID int) {
	//step 1:同步所有顶点的邻边
	vertID := vertexID - lw.WContext.GraphData.VertIDStart
	if len(lw.WContext.GraphData.Edges.GetInEdges(vertID))+len(lw.WContext.GraphData.Edges.GetOutEdges(vertID)) == 0 {
		return
	}
	if lw.WContext.Step == 1 {
		// scatter inedge and weight
		lw.inEdges[vertexID] = make(serialize.MapUint32Float32, len(lw.WContext.GraphData.Edges.GetInEdges(vertID)))
		for idx, edge := range lw.WContext.GraphData.Edges.GetInEdges(vertID) {
			// trim self loop
			if edge == serialize.SUint32(vertexID) {
				continue
			}
			var weight serialize.SFloat32 = 1
			if lw.useProperty {
				switch lw.propertyType {
				case structure.ValueTypeInt32:
					weight = serialize.SFloat32(lw.WContext.GraphData.InEdgesProperty.GetInt32Value(lw.edgeProperty, vertID, uint32(idx)))
				case structure.ValueTypeFloat32:
					weight = lw.WContext.GraphData.InEdgesProperty.GetFloat32Value(lw.edgeProperty, vertID, uint32(idx))
				}

			}
			if weight == 0 {
				continue
			}

			lw.inEdges[vertexID][edge] += weight
		}
	} else if lw.WContext.Step == 2 {
		// get outedge weight
		lw.neighborEdges[vertexID] = make(serialize.MapUint32Float32, len(lw.inEdges[vertexID]))
		trimMap := make(map[serialize.SUint32]struct{})
		trimMap[serialize.SUint32(vertexID)] = struct{}{}
		for edge, weight := range lw.inEdges[vertexID] {
			lw.neighborEdges[vertexID][edge] += weight
		}
		for _, edge := range lw.WContext.GraphData.Edges.GetOutEdges(vertID) {
			if _, ok := trimMap[edge]; ok {
				continue
			}
			trimMap[edge] = struct{}{}
			wgt := lw.inEdges[edge][serialize.SUint32(vertexID)]
			if wgt == 0 {
				continue
			}
			lw.neighborEdges[vertexID][edge] += wgt
		}
	} else {
		if lw.firstStep {
			// Zero-weight edges are removed while building neighborEdges. A vertex
			// with no remaining effective edge is an identity community and must
			// not enter the modularity optimization path.
			if len(lw.neighborEdges[vertexID]) == 0 {
				return
			}
			//以vertex为基本单元计算
			currCommID := lw.firstStepCommID[vertexID]
			kI := lw.firstStepKI[vertexID]

			//neighborCommKIin  计算neighbor社区的KIin
			neighborCommKVInInPID := make(map[serialize.SUint32]float64, len(lw.neighborEdges[vertexID]))

			for neighbor, weight := range lw.neighborEdges[vertexID] {
				neighborCommID := lw.firstStepCommID[neighbor]
				neighborCommKVInInPID[neighborCommID] += float64(weight)
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

func (lw *LouvainWeightedWorker) AfterStep() {
	if lw.WContext.Step >= 3 {
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

func (lw *LouvainWeightedWorker) OutputValueType() string {
	return common.HgValueTypeInt
}

func (lw *LouvainWeightedWorker) optimizeMem(currCommIDs map[serialize.SUint32]struct{}) {
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

func (lw *LouvainWeightedWorker) deleteEmptyComm() {
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

func (lw *LouvainWeightedWorker) calDeltaQ(kVIn, sigmaTot, kI float64) float64 {
	//DeltaQ = k_v_in - tot * k_v / m
	//各元素物理意义：
	//k_v_in: 当前点指向目标点所在社区的边权值之和
	//tot: 目标点所在社区内边外边权重之和（如果当前点和目标点处在同一个社区，去要减掉一个k_v）
	//k_v: 当前点内外度之和
	//m: 全图边权重之和,已处理在resolution之内
	return kVIn - lw.resolution*sigmaTot*kI
}

func (lw *LouvainWeightedWorker) calModularity() float64 {
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

func (lw *LouvainWeightedWorker) initLouvainNode() {
	//gen a new graph for new step
	//可以并行
	lw.nodes = make(map[serialize.SUint32]*louvainNode, len(lw.communities))
	lw.nodeID = identityCommunityIDs(lw.WContext.GraphData.Vertex.TotalVertexCount())
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
					for neighbor, weight := range lw.neighborEdges[vertex] {
						if _, ok := comm.node[neighbor]; ok {
							newNodesInPID[commID].KIn += float64(weight)
							continue
						}
						newNodesInPID[commID].neighbors[lw.firstStepCommID[neighbor]] += float64(weight)
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

func (lw *LouvainWeightedWorker) genNewGraph() {
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

type LouvainWeightedMaster struct {
	compute.MasterComputerBase
	//阈值，总模块度值的变化是否小于阈值判断是否退出算法。
	threshold float64
	//前一个收敛完的迭代得到的模块度
	prevModValue serialize.SFloat32
	louvainStep  int
	maxStep      int
}

func (lm *LouvainWeightedMaster) Init() error {
	lm.threshold = options.GetFloat(lm.MContext.Params, "louvain.threshold")
	if err := validateNonNegativeFinite("louvain.threshold", lm.threshold); err != nil {
		return err
	}
	lm.maxStep = options.GetInt(lm.MContext.Params, "louvain.step")
	if err := validatePositiveInt("louvain.step", lm.maxStep); err != nil {
		return err
	}
	lm.prevModValue = math.MinInt32
	lm.louvainStep = 1
	return nil
}

func (lm *LouvainWeightedMaster) Compute() bool {
	//对比模块度变化，小于阈值则提前退出
	if lm.MContext.Step >= 3 {
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
		if float64(modValue-lm.prevModValue) <= lm.threshold || lm.louvainStep >= lm.maxStep {
			lm.prevModValue = modValue
			return false
		} else {
			lm.prevModValue = modValue
		}
	}

	return true
}

func (lm *LouvainWeightedMaster) Statistics() map[string]any {
	return map[string]any{
		"modularity_in_louvain_weighted": lm.prevModValue,
	}
}
