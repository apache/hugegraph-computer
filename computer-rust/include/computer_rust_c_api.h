/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef HUGEGRAPH_COMPUTER_RUST_C_API_H
#define HUGEGRAPH_COMPUTER_RUST_C_API_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct GraphHandle GraphHandle;

/**
 * Creates a new GraphHandle instance with the specified number of vertices.
 */
GraphHandle* computer_graph_create(uint32_t num_vertices);

/**
 * Adds a directed edge from src to dst with a weight.
 * @return 0 on success.
 * @return -1 if handle is NULL, endpoints src/dst are >= num_vertices, or weight < 0.0 or non-finite.
 * @return -2 if graph has already been finalized.
 */
int32_t computer_graph_add_edge(GraphHandle* handle, uint32_t src, uint32_t dst, double weight);

/**
 * Finalizes graph topology into Compressed Sparse Row (CSR) structure.
 * @return 0 on success, -1 if handle is NULL.
 */
int32_t computer_graph_finalize(GraphHandle* handle);

/**
 * Computes PageRank on the CSR graph structure.
 * Results array must be allocated by caller with capacity >= num_vertices.
 * @return 0 on success.
 * @return -1 if handle or out_scores is NULL.
 * @return -2 if graph is not finalized (CSR missing).
 * @return -3 if out_capacity < num_vertices.
 * @return -4 if damping_factor or tolerance is invalid (non-finite, negative, or damping > 1.0).
 */
int32_t computer_graph_compute_pagerank(
    const GraphHandle* handle,
    double damping_factor,
    uint32_t max_iterations,
    double tolerance,
    double* out_scores,
    uint32_t out_capacity
);

/**
 * Frees the GraphHandle resources.
 */
void computer_graph_free(GraphHandle* handle);

/**
 * Returns the version string of the Rust kernel library.
 * Pointer references process-lifetime static storage and remains valid across threads.
 */
const char* computer_kernel_version(void);

#ifdef __cplusplus
}
#endif

#endif /* HUGEGRAPH_COMPUTER_RUST_C_API_H */
