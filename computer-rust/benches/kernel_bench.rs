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

use criterion::{criterion_group, criterion_main, Criterion};
use hugegraph_computer_rust::fixtures::dataset::GraphFixture;
use hugegraph_computer_rust::kernel::pagerank::PageRankKernel;

fn bench_pagerank(c: &mut Criterion) {
    let fixture = GraphFixture::synthetic_powerlaw(1000, 10);
    let csr = fixture.to_csr();
    let kernel = PageRankKernel::new(0.85, 20, 1e-4);

    c.bench_function("pagerank_1k_vertices", |b| b.iter(|| kernel.compute(&csr)));
}

criterion_group!(benches, bench_pagerank);
criterion_main!(benches);
