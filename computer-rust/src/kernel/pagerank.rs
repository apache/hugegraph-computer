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

use crate::kernel::csr::CsrGraph;

#[derive(Debug, Clone)]
pub struct PageRankKernel {
    damping_factor: f64,
    max_iterations: u32,
    tolerance: f64,
}

impl PageRankKernel {
    pub fn try_new(
        damping_factor: f64,
        max_iterations: u32,
        tolerance: f64,
    ) -> Result<Self, String> {
        if !damping_factor.is_finite() || damping_factor < 0.0 || damping_factor > 1.0 {
            return Err(format!(
                "Invalid damping_factor: {}. Must be finite and in range [0.0, 1.0]",
                damping_factor
            ));
        }
        if !tolerance.is_finite() || tolerance < 0.0 {
            return Err(format!(
                "Invalid tolerance: {}. Must be finite non-negative number",
                tolerance
            ));
        }
        Ok(Self {
            damping_factor,
            max_iterations,
            tolerance,
        })
    }

    pub fn new(damping_factor: f64, max_iterations: u32, tolerance: f64) -> Self {
        Self::try_new(damping_factor, max_iterations, tolerance)
            .expect("Failed to initialize PageRankKernel due to invalid parameters")
    }

    #[allow(clippy::needless_range_loop)]
    pub fn compute(&self, graph: &CsrGraph) -> Vec<f64> {
        let num_vertices = graph.num_vertices() as usize;
        if num_vertices == 0 {
            return Vec::new();
        }

        let initial_rank = 1.0 / (num_vertices as f64);
        let mut ranks = vec![initial_rank; num_vertices];
        let mut next_ranks = vec![0.0; num_vertices];

        let teleport = (1.0 - self.damping_factor) / (num_vertices as f64);

        for _iter in 0..self.max_iterations {
            next_ranks.fill(0.0);
            let mut dangling_sum = 0.0;

            for v in 0..num_vertices {
                let out_degree = graph.out_degree(v as u32);
                if out_degree == 0 {
                    dangling_sum += ranks[v];
                } else {
                    let share = ranks[v] / (out_degree as f64);
                    let (neighbors, _) = graph.out_edges(v as u32);
                    for &target in neighbors {
                        next_ranks[target as usize] += share;
                    }
                }
            }

            let dangling_share = self.damping_factor * (dangling_sum / (num_vertices as f64));
            let mut max_diff = 0.0f64;

            for v in 0..num_vertices {
                let new_rank = teleport + dangling_share + self.damping_factor * next_ranks[v];
                let diff = (new_rank - ranks[v]).abs();
                if diff > max_diff {
                    max_diff = diff;
                }
                ranks[v] = new_rank;
            }

            if max_diff < self.tolerance {
                break;
            }
        }

        ranks
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pagerank_computation() {
        let edges = vec![(0, 1, 1.0), (1, 2, 1.0), (2, 0, 1.0)];
        let graph = CsrGraph::from_edges(3, &edges);
        let pr = PageRankKernel::new(0.85, 100, 1e-6);
        let ranks = pr.compute(&graph);

        assert_eq!(ranks.len(), 3);
        let sum: f64 = ranks.iter().sum();
        assert!((sum - 1.0).abs() < 1e-4);
        assert!((ranks[0] - ranks[1]).abs() < 1e-4);
        assert!((ranks[1] - ranks[2]).abs() < 1e-4);
    }

    #[test]
    fn test_pagerank_parameter_validation() {
        assert!(PageRankKernel::try_new(1.5, 100, 1e-6).is_err());
        assert!(PageRankKernel::try_new(-0.1, 100, 1e-6).is_err());
        assert!(PageRankKernel::try_new(f64::NAN, 100, 1e-6).is_err());
        assert!(PageRankKernel::try_new(0.85, 100, -1e-6).is_err());
        assert!(PageRankKernel::try_new(0.85, 100, f64::NAN).is_err());
        assert!(PageRankKernel::try_new(0.85, 100, 1e-6).is_ok());
    }
}
