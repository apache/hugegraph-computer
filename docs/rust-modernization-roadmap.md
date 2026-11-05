<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# HugeGraph Computer & Vermeer: Rust Modernization Roadmap (#355)

## Overview

This roadmap details the incremental modernization strategy for graph computing components in **HugeGraph Computer** and **Vermeer**. The initiative focuses on high-performance kernels, data movement, memory efficiency, and operational simplicity where Rust provides a measurable advantage over Java (JVM GC overhead) and Go.

> **Note:** This initiative is an incremental enhancement—not a wholesale replacement of existing systems. Existing Java/Go algorithms, data formats, and deployment paths remain the compatibility and baseline benchmark.

---

## Architectural Principles & Guardrails

1. **Zero Downtime / Seamless Coexistence:** Java and Go baselines are preserved with automatic fallback if native Rust modules are unavailable.
2. **Result Parity & Tolerance:** Differential correctness testing enforces $L_1$-distance $\le 10^{-6}$ against ground-truth algorithm outputs.
3. **Bounded Leaf Modules:** Incremental modernization targets encapsulated primitives (CSR memory layout, PageRank kernel, differential checkers) in Phase 1 before expanding to secondary algorithms or host engine bindings.
4. **Stable Interoperability Layer:** Exported via standard C-ABI (`computer_rust_c_api.h`) for future host bindings (JNI in Java `computer-core` and CGO/gRPC in `vermeer`).

---

## Component Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    User / Applications                      │
└──────────────────────────────┬──────────────────────────────┘
                               │
            ┌──────────────────┴──────────────────┐
            ▼                                     ▼
┌─────────────────────────┐           ┌─────────────────────────┐
│ HugeGraph Computer      │           │ Vermeer                 │
│ (Java / BSP Pregel)     │           │ (Go / In-Memory Engine) │
└───────────┬─────────────┘           └───────────┬─────────────┘
            │ JNI / FFI (Phase 3)                 │ CGO / FFI (Phase 3)
            └──────────────────┬──────────────────┘
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                computer-rust (C-ABI Layer)                  │
│  ┌──────────────────┬──────────────────┬─────────────────┐  │
│  │ CSR Graph Layout │ PageRank Kernel  │ Dataset Fixtures│  │
│  ├──────────────────┼──────────────────┼─────────────────┤  │
│  │ C-ABI Exports    │ Differential PR  │ Criterion Bench │  │
│  └──────────────────┴──────────────────┴─────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

## C-ABI Interoperability & Return Code Contracts

The exported C-ABI layer (`computer_rust_c_api.h`) defines standard return codes and lifetime guarantees across language boundaries:

### Error Codes
* `0`: Success.
* `-1`: Null handle or invalid boundary parameters (out-of-bounds `src`/`dst` endpoints, negative or non-finite edge weights).
* `-2`: State error (attempting to add edges to a finalized graph, or missing CSR during computation).
* `-3`: Insufficient output buffer capacity (`out_capacity < num_vertices`).
* `-4`: Invalid algorithm parameters (non-finite or out-of-bounds `damping_factor`, or negative `tolerance`).

### Lifetime & Thread Safety
* `computer_kernel_version()` returns a `*const c_char` pointing to process-lifetime static storage (`OnceLock<CString>`). The pointer is thread-safe and remains valid throughout application lifetime.

### Fallback Adapters (Planned for Phase 3)
* Host integration bridges in `computer-core` (Java) and `vermeer` (Go) will provide transparent fallback execution paths. When native libraries are absent or opt-in preview flags are disabled, graph algorithms fall back transparently to pure Java/Go execution while enforcing identical parameter validation.

---

## Newcomer-Friendly Child Task Breakdown

The following tasks are split into isolated, newcomer-friendly issues for community contributors:

| Task ID | Component | Title | Description | Target Skills |
|---------|-----------|-------|-------------|---------------|
| `#355-1` | `computer-rust` | SSSP, WCC & LPA Kernels | Port Shortest Path (SSSP), Weakly Connected Components (WCC), and Label Propagation (LPA) to CSR Rust kernel. | Rust, Graph Algorithms |
| `#355-2` | `computer-core` | Java JNI Bridge & Dynamic Loader | Package platform-specific native libraries (`.so`, `.dylib`, `.dll`) into JAR artifacts with automated JNI extraction and fallback. | Java, JNI, Build Automation |
| `#355-3` | `vermeer` | Go CGO Bridge & Benchmark | Connect Vermeer Go compute engine to Rust C-ABI via CGO with performance benchmarking against Go baseline. | Go, CGO, Benchmarking |
| `#355-4` | `computer-rust` | Parquet / Arrow Memory Mapped I/O | Add zero-copy memory-mapped file reader for CSR graph initialization. | Rust, Memory Mapping |

---

## Verification & Parity Guidelines

To verify algorithm outputs against ground-truth baselines:

```bash
# Run Rust kernel tests and differential parity checks
cd computer-rust
cargo test --all-targets

# Run Criterion benchmark harness
cargo bench
```
