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

#[derive(Debug, Clone, Default)]
pub struct Edge {
    pub target: u32,
    pub weight: f64,
}

#[derive(Debug, Clone)]
pub struct CsrGraph {
    num_vertices: u32,
    row_offsets: Vec<usize>,
    column_indices: Vec<u32>,
    edge_weights: Vec<f64>,
}

impl CsrGraph {
    pub fn new(num_vertices: u32) -> Self {
        Self {
            num_vertices,
            row_offsets: vec![0; (num_vertices + 1) as usize],
            column_indices: Vec::new(),
            edge_weights: Vec::new(),
        }
    }

    pub fn from_edges(num_vertices: u32, edges: &[(u32, u32, f64)]) -> Self {
        let mut degree = vec![0; num_vertices as usize];
        for &(src, dst, _weight) in edges {
            if src < num_vertices && dst < num_vertices {
                degree[src as usize] += 1;
            }
        }

        let mut row_offsets = vec![0; (num_vertices + 1) as usize];
        for i in 0..num_vertices as usize {
            row_offsets[i + 1] = row_offsets[i] + degree[i];
        }

        let total_edges = row_offsets[num_vertices as usize];
        let mut column_indices = vec![0; total_edges];
        let mut edge_weights = vec![0.0; total_edges];
        let mut current_pos = row_offsets.clone();

        for &(src, dst, weight) in edges {
            if src < num_vertices && dst < num_vertices {
                let pos = current_pos[src as usize];
                column_indices[pos] = dst;
                edge_weights[pos] = weight;
                current_pos[src as usize] += 1;
            }
        }

        Self {
            num_vertices,
            row_offsets,
            column_indices,
            edge_weights,
        }
    }

    pub fn num_vertices(&self) -> u32 {
        self.num_vertices
    }

    pub fn num_edges(&self) -> usize {
        self.column_indices.len()
    }

    pub fn out_degree(&self, vertex: u32) -> usize {
        if vertex >= self.num_vertices {
            return 0;
        }
        let v = vertex as usize;
        self.row_offsets[v + 1] - self.row_offsets[v]
    }

    pub fn out_edges(&self, vertex: u32) -> (&[u32], &[f64]) {
        if vertex >= self.num_vertices {
            return (&[], &[]);
        }
        let v = vertex as usize;
        let start = self.row_offsets[v];
        let end = self.row_offsets[v + 1];
        (
            &self.column_indices[start..end],
            &self.edge_weights[start..end],
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_csr_graph_creation() {
        let edges = vec![(0, 1, 1.0), (0, 2, 2.0), (1, 2, 0.5)];
        let graph = CsrGraph::from_edges(3, &edges);

        assert_eq!(graph.num_vertices(), 3);
        assert_eq!(graph.num_edges(), 3);
        assert_eq!(graph.out_degree(0), 2);
        assert_eq!(graph.out_degree(1), 1);
        assert_eq!(graph.out_degree(2), 0);

        let (neighbors, weights) = graph.out_edges(0);
        assert_eq!(neighbors, &[1, 2]);
        assert_eq!(weights, &[1.0, 2.0]);
    }

    #[test]
    fn test_csr_invalid_endpoints() {
        let edges = vec![(0, 99, 1.0), (0, 1, 2.0)];
        let graph = CsrGraph::from_edges(2, &edges);
        assert_eq!(graph.num_vertices(), 2);
        assert_eq!(graph.num_edges(), 1);
        assert_eq!(graph.out_degree(0), 1);
        let (neighbors, weights) = graph.out_edges(0);
        assert_eq!(neighbors, &[1]);
        assert_eq!(weights, &[2.0]);
    }
}
