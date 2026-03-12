# Apache HugeGraph-Computer (Java)

<!-- status:deepwiki-budget:0 -->

HugeGraph-Computer is a distributed graph processing framework implementing the [Pregel](https://kowshik.github.io/JPregel/pregel_paper.pdf) model (BSP - Bulk Synchronous Parallel). It runs on Kubernetes or YARN clusters and integrates with HugeGraph for graph input/output.

## Features

- **Distributed MPP Computing**: Massively parallel graph processing across cluster nodes
- **BSP Model**: Algorithm execution through iterative supersteps with global synchronization
- **Auto Memory Management**: Automatic spill to disk when memory is insufficient - never OOM
- **Flexible Data Sources**: Load from HugeGraph or HDFS, output to HugeGraph or HDFS
- **Easy Algorithm Development**: Focus on single-vertex logic without worrying about distribution
- **Production-Ready**: Battle-tested on billion-scale graphs with Kubernetes integration

## Architecture

### Module Structure

```
┌─────────────────────────────────────────────────────────────┐
│                    HugeGraph-Computer                       │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────┐   │
│  │                   computer-driver                    │   │
│  │              (Job Submission & Coordination)         │   │
│  └─────────────────────────┬───────────────────────────┘   │
│                            │                                │
│  ┌─────────────────────────┼───────────────────────────┐   │
│  │         Deployment Layer (choose one)               │   │
│  │  ┌──────────────┐              ┌──────────────┐     │   │
│  │  │ computer-k8s │              │ computer-yarn│     │   │
│  │  └──────────────┘              └──────────────┘     │   │
│  └─────────────────────────┬───────────────────────────┘   │
│                            │                                │
│  ┌─────────────────────────┼───────────────────────────┐   │
│  │                   computer-core                      │   │
│  │        (WorkerService, MasterService, BSP)          │   │
│  │  ┌──────────────────────────────────────────────┐   │   │
│  │  │  Managers: Message, Aggregation, Snapshot... │   │   │
│  │  └──────────────────────────────────────────────┘   │   │
│  └─────────────────────────┬───────────────────────────┘   │
│                            │                                │
│  ┌─────────────────────────┼───────────────────────────┐   │
│  │              computer-algorithm                      │   │
│  │    (PageRank, LPA, WCC, SSSP, TriangleCount...)     │   │
│  └─────────────────────────┬───────────────────────────┘   │
│                            │                                │
│  ┌─────────────────────────┴───────────────────────────┐   │
│  │                    computer-api                      │   │
│  │    (Computation, Vertex, Edge, Aggregator, Value)   │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### Module Descriptions

| Module | Description |
|--------|-------------|
| **computer-api** | Public interfaces for algorithm development (`Computation`, `Vertex`, `Edge`, `Aggregator`, `Combiner`) |
| **computer-core** | Runtime implementation (WorkerService, MasterService, messaging, BSP coordination, memory management) |
| **computer-algorithm** | Built-in graph algorithms (45+ implementations) |
| **computer-driver** | Job submission and driver-side coordination |
| **computer-k8s** | Kubernetes deployment integration |
| **computer-yarn** | YARN deployment integration |
| **computer-k8s-operator** | Kubernetes operator for job lifecycle management |
| **computer-dist** | Distribution packaging and assembly |
| **computer-test** | Integration tests and unit tests |

## Prerequisites

- **JDK 11** or later (for building and running)
- **Maven 3.5+** for building
- **Kubernetes cluster** or **YARN cluster** for deployment
- **etcd** for BSP coordination (configured via `BSP_ETCD_URL`)

**Note**: For K8s-operator module development, run `mvn clean install` in `computer-k8s-operator` first to generate CRD classes.

## Quick Start

### Build from Source

```bash
cd computer

# Compile (skip javadoc for faster builds)
mvn clean compile -Dmaven.javadoc.skip=true

# Package (skip tests for faster packaging)
mvn clean package -DskipTests
```

### Run Tests

```bash
# Unit tests
mvn test -P unit-test

# Integration tests (requires etcd, K8s, HugeGraph)
mvn test -P integrate-test

# Run specific test class
mvn test -P unit-test -Dtest=ClassName

# Run specific test method
mvn test -P unit-test -Dtest=ClassName#methodName
```

### License Check

```bash
mvn apache-rat:check
```

### Deploy on Kubernetes

#### 1. Configure Job

Create `job-config.properties`:

```properties
# Algorithm class
algorithm.class=org.apache.hugegraph.computer.algorithm.centrality.pagerank.PageRank

# HugeGraph connection
hugegraph.url=http://hugegraph-server:8080
hugegraph.graph=hugegraph

# K8s configuration
k8s.namespace=default
k8s.image=hugegraph/hugegraph-computer:latest
k8s.master.cpu=2
k8s.master.memory=4Gi
k8s.worker.replicas=3
k8s.worker.cpu=4
k8s.worker.memory=8Gi

# BSP coordination (etcd)
bsp.etcd.url=http://etcd-cluster:2379

# Algorithm parameters (PageRank example)
# Alpha parameter (1 - damping factor), default: 0.15
page_rank.alpha=0.85

# Maximum supersteps (iterations), controlled by BSP framework
bsp.max_superstep=20

# L1 norm difference threshold for convergence, default: 0.00001
pagerank.l1DiffThreshold=0.0001
```

#### 2. Submit Job

```bash
java -jar computer-driver/target/computer-driver-${VERSION}.jar \
  --config job-config.properties
```

#### 3. Monitor Job

```bash
# Check pod status
kubectl get pods -n default

# View master logs
kubectl logs hugegraph-computer-master-xxx -n default

# View worker logs
kubectl logs hugegraph-computer-worker-0 -n default
```

### Deploy on YARN

**Note**: YARN deployment support is under development. Use Kubernetes for production deployments.

## Available Algorithms

### Centrality Algorithms

| Algorithm | Class | Description |
|-----------|-------|-------------|
| PageRank | `algorithm.centrality.pagerank.PageRank` | Standard PageRank |
| Personalized PageRank | `algorithm.centrality.pagerank.PersonalizedPageRank` | Source-specific PageRank |
| Betweenness Centrality | `algorithm.centrality.betweenness.BetweennessCentrality` | Shortest-path-based centrality |
| Closeness Centrality | `algorithm.centrality.closeness.ClosenessCentrality` | Average distance centrality |
| Degree Centrality | `algorithm.centrality.degree.DegreeCentrality` | In/out degree counting |

### Community Detection

| Algorithm | Class | Description |
|-----------|-------|-------------|
| LPA | `algorithm.community.lpa.Lpa` | Label Propagation Algorithm |
| WCC | `algorithm.community.wcc.Wcc` | Weakly Connected Components |
| Louvain | `algorithm.community.louvain.Louvain` | Modularity-based community detection |
| K-Core | `algorithm.community.kcore.KCore` | K-core decomposition |

### Path Finding

| Algorithm | Class | Description |
|-----------|-------|-------------|
| SSSP | `algorithm.path.sssp.Sssp` | Single Source Shortest Path |
| BFS | `algorithm.traversal.bfs.Bfs` | Breadth-First Search |
| Rings | `algorithm.path.rings.Rings` | Cycle/ring detection |

### Graph Structure

| Algorithm | Class | Description |
|-----------|-------|-------------|
| Triangle Count | `algorithm.trianglecount.TriangleCount` | Count triangles |
| Clustering Coefficient | `algorithm.clusteringcoefficient.ClusteringCoefficient` | Local clustering measure |

**Full algorithm list**: See `computer-algorithm/src/main/java/org/apache/hugegraph/computer/algorithm/`

## Developing Custom Algorithms

### Algorithm Contract

Algorithms implement the `Computation` interface from `computer-api`:

```java
package org.apache.hugegraph.computer.core.worker;

public interface Computation<M extends Value> {
    /**
     * Initialization at superstep 0
     */
    void compute0(ComputationContext context, Vertex vertex);

    /**
     * Message processing in subsequent supersteps
     */
    void compute(ComputationContext context, Vertex vertex, Iterator<M> messages);
}
```

### Example: Simple PageRank

> **NOTE**: This is a simplified example showing the key concepts.
> For the complete implementation including all required methods (`name()`, `category()`, `init()`, etc.),
> see: `computer/computer-algorithm/src/main/java/org/apache/hugegraph/computer/algorithm/centrality/pagerank/PageRank.java`

```java
package org.apache.hugegraph.computer.algorithm.centrality.pagerank;

import org.apache.hugegraph.computer.core.worker.Computation;
import org.apache.hugegraph.computer.core.worker.ComputationContext;

public class PageRank implements Computation<DoubleValue> {

    public static final String OPTION_ALPHA = "pagerank.alpha";
    public static final String OPTION_MAX_ITERATIONS = "pagerank.max_iterations";

    private double alpha;
    private int maxIterations;

    @Override
    public void init(Config config) {
        this.alpha = config.getDouble(OPTION_ALPHA, 0.85);
        this.maxIterations = config.getInt(OPTION_MAX_ITERATIONS, 20);
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        // Initialize: set initial PR value
        vertex.value(new DoubleValue(1.0));

        // Send PR to neighbors
        int edgeCount = vertex.numEdges();
        if (edgeCount > 0) {
            double contribution = 1.0 / edgeCount;
            context.sendMessageToAllEdges(vertex, new DoubleValue(contribution));
        }
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex, Iterator<DoubleValue> messages) {
        // Sum incoming PR contributions
        double sum = 0.0;
        while (messages.hasNext()) {
            sum += messages.next().value();
        }

        // Calculate new PR value
        double newPR = (1.0 - alpha) + alpha * sum;
        vertex.value(new DoubleValue(newPR));

        // Send to neighbors if not converged
        if (context.superstep() < maxIterations) {
            int edgeCount = vertex.numEdges();
            if (edgeCount > 0) {
                double contribution = newPR / edgeCount;
                context.sendMessageToAllEdges(vertex, new DoubleValue(contribution));
            }
        } else {
            vertex.inactivate();
        }
    }
}
```

### Key Concepts

#### 1. Supersteps

- **Superstep 0**: Initialization via `compute0()`
- **Superstep 1+**: Message processing via `compute()`
- **Barrier Synchronization**: All workers complete superstep N before starting N+1

#### 2. Message Passing

```java
// Send to specific vertex
context.sendMessage(targetId, new DoubleValue(1.0));

// Send to all outgoing edges
context.sendMessageToAllEdges(vertex, new DoubleValue(1.0));
```

#### 3. Aggregators

Global state shared across all workers:

```java
// Register aggregator in compute0()
context.registerAggregator("sum", new DoubleValue(0.0), SumAggregator.class);

// Write to aggregator
context.aggregateValue("sum", new DoubleValue(vertex.value()));

// Read aggregator value (available in next superstep)
DoubleValue total = context.aggregatedValue("sum");
```

#### 4. Combiners

Reduce message volume by combining messages at sender:

```java
public class SumCombiner implements Combiner<DoubleValue> {
    @Override
    public void combine(DoubleValue v1, DoubleValue v2, DoubleValue result) {
        result.value(v1.value() + v2.value());
    }
}
```

### Algorithm Development Workflow

1. **Implement `Computation` interface** in `computer-algorithm`
2. **Add configuration options** with `OPTION_*` constants
3. **Implement `compute0()` for initialization**
4. **Implement `compute()` for message processing**
5. **Configure in job properties**:
   ```properties
   algorithm.class=com.example.MyAlgorithm
   myalgorithm.param1=value1
   ```
6. **Build and test**:
   ```bash
   mvn clean package -DskipTests
   ```

## BSP Coordination

HugeGraph-Computer uses etcd for BSP barrier synchronization:

### BSP Lifecycle (per superstep)

1. **Worker Prepare**: `workerStepPrepareDone` → `waitMasterStepPrepareDone`
2. **Compute Phase**: Workers process vertices and messages locally
3. **Worker Compute Done**: `workerStepComputeDone` → `waitMasterStepComputeDone`
4. **Aggregation**: Aggregators combine global state
5. **Worker Step Done**: `workerStepDone` → `waitMasterStepDone` (master returns `SuperstepStat`)

### Manager Pattern

`WorkerService` composes multiple managers with lifecycle hooks:

- `MessageSendManager`: Outgoing message buffering and sending
- `MessageRecvManager`: Incoming message receiving and sorting
- `WorkerAggrManager`: Aggregator value collection
- `DataServerManager`: Inter-worker data transfer
- `SortManagers`: Message and edge sorting
- `SnapshotManager`: Checkpoint creation

All managers implement:
- `initAll()`: Initialize before first superstep
- `beforeSuperstep()`: Prepare for superstep
- `afterSuperstep()`: Cleanup after superstep
- `closeAll()`: Shutdown cleanup

## Configuration Reference

### Job Configuration

```properties
# === Algorithm ===
algorithm.class=<fully qualified class name>
algorithm.message_class=<message value class>
algorithm.result_class=<result value class>

# === HugeGraph Input ===
hugegraph.url=http://localhost:8080
hugegraph.graph=hugegraph
hugegraph.input.vertex_label=person
hugegraph.input.edge_label=knows
hugegraph.input.filter=<gremlin filter>

# === HugeGraph Output ===
hugegraph.output.vertex_property=pagerank_value
hugegraph.output.edge_property=<property name>

# === HDFS Input ===
input.hdfs.path=/graph/input
input.hdfs.format=json

# === HDFS Output ===
output.hdfs.path=/graph/output
output.hdfs.format=json

# === Worker Resources ===
worker.count=3
worker.memory=8Gi
worker.cpu=4
worker.thread_count=<cpu cores>

# === BSP Coordination ===
bsp.etcd.url=http://etcd:2379
bsp.max_superstep=100
bsp.log_interval=10

# === Memory Management ===
worker.data.dirs=/data1,/data2
worker.write_buffer_size=134217728
worker.max_spill_size=1073741824
```

## Memory Management

Computer auto-manages memory to prevent OOM:

1. **In-Memory Buffering**: Vertices, edges, messages buffered in memory
2. **Spill Threshold**: When memory usage exceeds threshold, spill to disk
3. **Disk Storage**: Configurable data directories (`worker.data.dirs`)
4. **Automatic Cleanup**: Spilled data cleaned after superstep completion

**Best Practice**: Allocate worker memory ≥ 2x graph size for optimal performance.

## Troubleshooting

### K8s CRD Classes Not Found

```bash
# Generate CRD classes first
cd computer-k8s-operator
mvn clean install
```

Generated classes appear in `computer-k8s/target/generated-sources/`.

### etcd Connection Errors

- Verify `bsp.etcd.url` is reachable from all pods
- Check etcd cluster health: `etcdctl endpoint health`
- Ensure firewall allows port 2379

### Out of Memory Errors

- Increase `worker.memory` in job config
- Reduce `worker.write_buffer_size` to trigger earlier spilling
- Increase `worker.count` to distribute graph across more workers

### Slow Convergence

- Check algorithm parameters (e.g., `pagerank.convergence_tolerance`)
- Monitor superstep logs for progress
- Consider using combiners to reduce message volume

## Important Files

| File | Description |
|------|-------------|
| `computer-api/.../Computation.java` | Algorithm interface contract (computer/computer-api/src/main/java/org/apache/hugegraph/computer/core/worker/Computation.java:25) |
| `computer-core/.../WorkerService.java` | Worker runtime orchestration (computer/computer-core/src/main/java/org/apache/hugegraph/computer/core/worker/WorkerService.java:1) |
| `computer-core/.../Bsp4Worker.java` | BSP coordination logic (computer/computer-core/src/main/java/org/apache/hugegraph/computer/core/bsp/Bsp4Worker.java:1) |
| `computer-algorithm/.../PageRank.java` | Example algorithm implementation (computer/computer-algorithm/src/main/java/org/apache/hugegraph/computer/algorithm/centrality/pagerank/PageRank.java:1) |

## Testing

### CI/CD Pipeline

The CI pipeline (`.github/workflows/computer-ci.yml`) runs:

1. License check (Apache RAT)
2. Setup HDFS (Hadoop 3.3.2)
3. Setup Minikube/Kubernetes
4. Load test data into HugeGraph
5. Compile with JDK 11
6. Run integration tests (`-P integrate-test`)
7. Run unit tests (`-P unit-test`)
8. Upload coverage to Codecov

### Local Testing

```bash
# Setup test environment (etcd, HDFS, K8s)
cd computer-dist/src/assembly/travis
./start-etcd.sh
./start-hdfs.sh
./start-minikube.sh

# Run tests
cd ../../../../
mvn test -P integrate-test
```

## Links

- [Project Homepage](https://hugegraph.apache.org/docs/quickstart/hugegraph-computer/)
- [Main README](../README.md)
- [Vermeer (Go) README](../vermeer/README.md)
- [GitHub Issues](https://github.com/apache/hugegraph-computer/issues)

## Contributing

See the main [Contributing Guide](../README.md#contributing) for how to contribute.

## License

HugeGraph-Computer is licensed under [Apache 2.0 License](https://github.com/apache/hugegraph-computer/blob/master/LICENSE).
