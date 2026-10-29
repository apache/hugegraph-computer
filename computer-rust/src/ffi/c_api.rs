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

#![allow(clippy::not_unsafe_ptr_arg_deref)]

use crate::kernel::csr::CsrGraph;
use crate::kernel::pagerank::PageRankKernel;
use crate::RUST_KERNEL_VERSION;
use std::ffi::CString;
use std::os::raw::c_char;
use std::slice;
use std::sync::OnceLock;

pub struct GraphBuilder {
    num_vertices: u32,
    edges: Vec<(u32, u32, f64)>,
    csr: Option<CsrGraph>,
}

#[no_mangle]
pub extern "C" fn computer_graph_create(num_vertices: u32) -> *mut GraphBuilder {
    let builder = Box::new(GraphBuilder {
        num_vertices,
        edges: Vec::new(),
        csr: None,
    });
    Box::into_raw(builder)
}

#[no_mangle]
pub extern "C" fn computer_graph_add_edge(
    handle: *mut GraphBuilder,
    src: u32,
    dst: u32,
    weight: f64,
) -> i32 {
    if handle.is_null() {
        return -1;
    }
    let builder = unsafe { &mut *handle };
    if builder.csr.is_some() {
        return -2;
    }
    if src >= builder.num_vertices || dst >= builder.num_vertices {
        return -1;
    }
    if weight < 0.0 || !weight.is_finite() {
        return -1;
    }
    builder.edges.push((src, dst, weight));
    0
}

#[no_mangle]
pub extern "C" fn computer_graph_finalize(handle: *mut GraphBuilder) -> i32 {
    if handle.is_null() {
        return -1;
    }
    let builder = unsafe { &mut *handle };
    let csr = CsrGraph::from_edges(builder.num_vertices, &builder.edges);
    builder.csr = Some(csr);
    0
}

#[no_mangle]
pub extern "C" fn computer_graph_compute_pagerank(
    handle: *const GraphBuilder,
    damping_factor: f64,
    max_iterations: u32,
    tolerance: f64,
    out_scores: *mut f64,
    out_capacity: u32,
) -> i32 {
    if handle.is_null() || out_scores.is_null() {
        return -1;
    }
    let builder = unsafe { &*handle };
    let csr = match &builder.csr {
        Some(c) => c,
        None => return -2,
    };

    if out_capacity < csr.num_vertices() {
        return -3;
    }

    if !damping_factor.is_finite() || damping_factor < 0.0 || damping_factor > 1.0 {
        return -4;
    }
    if !tolerance.is_finite() || tolerance < 0.0 {
        return -4;
    }

    let kernel = PageRankKernel::new(damping_factor, max_iterations, tolerance);
    let ranks = kernel.compute(csr);

    let dest_slice = unsafe { slice::from_raw_parts_mut(out_scores, ranks.len()) };
    dest_slice.copy_from_slice(&ranks);
    0
}

#[no_mangle]
pub extern "C" fn computer_graph_free(handle: *mut GraphBuilder) {
    if !handle.is_null() {
        unsafe {
            let _ = Box::from_raw(handle);
        }
    }
}

static VERSION_C_STR: OnceLock<CString> = OnceLock::new();

#[no_mangle]
pub extern "C" fn computer_kernel_version() -> *const c_char {
    VERSION_C_STR
        .get_or_init(|| CString::new(RUST_KERNEL_VERSION).unwrap())
        .as_ptr()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_c_api_flow() {
        let handle = computer_graph_create(4);
        assert!(!handle.is_null());

        assert_eq!(computer_graph_add_edge(handle, 0, 1, 1.0), 0);
        assert_eq!(computer_graph_add_edge(handle, 1, 2, 1.0), 0);

        assert_eq!(computer_graph_finalize(handle), 0);

        let mut scores = vec![0.0; 4];
        assert_eq!(
            computer_graph_compute_pagerank(handle, 0.85, 50, 1e-6, scores.as_mut_ptr(), 4),
            0
        );

        let sum: f64 = scores.iter().sum();
        assert!((sum - 1.0).abs() < 1e-4);
        assert!(scores[0] > 0.0 && scores[1] > 0.0 && scores[2] > 0.0);

        computer_graph_free(handle);

        let ver_ptr = computer_kernel_version();
        assert!(!ver_ptr.is_null());
    }

    #[test]
    fn test_c_api_edge_validation_and_finalization() {
        let handle = computer_graph_create(2);
        assert!(!handle.is_null());

        assert_eq!(computer_graph_add_edge(handle, 99, 1, 1.0), -1);
        assert_eq!(computer_graph_add_edge(handle, 0, 99, 1.0), -1);

        assert_eq!(computer_graph_add_edge(handle, 0, 1, -1.0), -1);
        assert_eq!(computer_graph_add_edge(handle, 0, 1, f64::NAN), -1);

        assert_eq!(computer_graph_add_edge(handle, 0, 1, 1.0), 0);
        assert_eq!(computer_graph_finalize(handle), 0);

        assert_eq!(computer_graph_add_edge(handle, 0, 1, 1.0), -2);

        computer_graph_free(handle);
    }

    #[test]
    fn test_c_api_parameter_validation() {
        let handle = computer_graph_create(2);
        assert_eq!(computer_graph_add_edge(handle, 0, 1, 1.0), 0);
        assert_eq!(computer_graph_finalize(handle), 0);

        let mut scores = vec![0.0; 2];
        assert_eq!(
            computer_graph_compute_pagerank(handle, 1.5, 50, 1e-6, scores.as_mut_ptr(), 2),
            -4
        );
        assert_eq!(
            computer_graph_compute_pagerank(handle, f64::NAN, 50, 1e-6, scores.as_mut_ptr(), 2),
            -4
        );
        assert_eq!(
            computer_graph_compute_pagerank(handle, 0.85, 50, -1.0, scores.as_mut_ptr(), 2),
            -4
        );

        computer_graph_free(handle);
    }

    #[test]
    fn test_c_api_version_static_lifetime() {
        let ver_ptr1 = computer_kernel_version();
        let handle = std::thread::spawn(|| computer_kernel_version() as usize);
        let ver_ptr2 = handle.join().unwrap() as *const c_char;
        assert_eq!(ver_ptr1, ver_ptr2);
        let ver_str = unsafe { std::ffi::CStr::from_ptr(ver_ptr1) }
            .to_str()
            .unwrap();
        assert_eq!(ver_str, RUST_KERNEL_VERSION);
    }
}
