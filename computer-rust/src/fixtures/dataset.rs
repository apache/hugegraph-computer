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

pub struct GraphFixture {
    pub name: String,
    pub num_vertices: u32,
    pub edges: Vec<(u32, u32, f64)>,
}

impl GraphFixture {
    /// Returns the Zachary's Karate Club representative graph dataset fixture.
    pub fn karate_club() -> Self {
        let edges = vec![
            (0, 1, 1.0),
            (0, 2, 1.0),
            (0, 3, 1.0),
            (0, 4, 1.0),
            (0, 5, 1.0),
            (0, 6, 1.0),
            (0, 7, 1.0),
            (0, 8, 1.0),
            (0, 10, 1.0),
            (0, 11, 1.0),
            (0, 12, 1.0),
            (0, 13, 1.0),
            (0, 17, 1.0),
            (0, 19, 1.0),
            (0, 21, 1.0),
            (0, 31, 1.0),
            (1, 2, 1.0),
            (1, 3, 1.0),
            (1, 7, 1.0),
            (1, 13, 1.0),
            (1, 17, 1.0),
            (1, 19, 1.0),
            (1, 21, 1.0),
            (1, 30, 1.0),
            (2, 3, 1.0),
            (2, 7, 1.0),
            (2, 8, 1.0),
            (2, 9, 1.0),
            (2, 13, 1.0),
            (2, 27, 1.0),
            (2, 28, 1.0),
            (2, 32, 1.0),
            (3, 7, 1.0),
            (3, 12, 1.0),
            (3, 13, 1.0),
        ];
        Self {
            name: "karate_club".to_string(),
            num_vertices: 34,
            edges,
        }
    }

    /// Generates a synthetic power-law graph dataset fixture for baseline testing.
    pub fn synthetic_powerlaw(num_vertices: u32, avg_degree: u32) -> Self {
        let mut edges = Vec::new();
        for src in 0..num_vertices {
            let out_degree = avg_degree + (src % 5);
            for i in 0..out_degree {
                let dst = (src + i * 7 + 1) % num_vertices;
                if src != dst {
                    edges.push((src, dst, 1.0));
                }
            }
        }
        Self {
            name: format!("synthetic_powerlaw_v{}", num_vertices),
            num_vertices,
            edges,
        }
    }

    pub fn to_csr(&self) -> CsrGraph {
        CsrGraph::from_edges(self.num_vertices, &self.edges)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_karate_club_fixture() {
        let fixture = GraphFixture::karate_club();
        assert_eq!(fixture.num_vertices, 34);
        assert!(!fixture.edges.is_empty());
        let csr = fixture.to_csr();
        assert_eq!(csr.num_vertices(), 34);
    }
}
