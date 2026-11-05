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

pub struct DifferentialTolerance;

impl DifferentialTolerance {
    pub fn l1_distance(actual: &[f64], expected: &[f64]) -> Result<f64, String> {
        if actual.len() != expected.len() {
            return Err(format!(
                "Vector length mismatch: actual len {}, expected len {}",
                actual.len(),
                expected.len()
            ));
        }

        let mut l1 = 0.0;
        for i in 0..actual.len() {
            if !actual[i].is_finite() || !expected[i].is_finite() {
                return Err(format!(
                    "Non-finite value detected at index {}: actual = {}, expected = {}",
                    i, actual[i], expected[i]
                ));
            }
            l1 += (actual[i] - expected[i]).abs();
        }

        if !l1.is_finite() {
            return Err("Calculated L1 distance is non-finite".to_string());
        }

        Ok(l1)
    }

    pub fn assert_parity(actual: &[f64], expected: &[f64], epsilon: f64) -> Result<(), String> {
        if actual.len() != expected.len() {
            return Err(format!(
                "Vector length mismatch: actual len {}, expected len {}",
                actual.len(),
                expected.len()
            ));
        }

        for i in 0..actual.len() {
            if !actual[i].is_finite() || !expected[i].is_finite() {
                return Err(format!(
                    "Non-finite value detected at index {}: actual = {}, expected = {}",
                    i, actual[i], expected[i]
                ));
            }
            let diff = (actual[i] - expected[i]).abs();
            if !diff.is_finite() || diff > epsilon {
                return Err(format!(
                    "Parity failed at index {}: actual = {}, expected = {}, diff = {} > epsilon {}",
                    i, actual[i], expected[i], diff, epsilon
                ));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_differential_tolerance() {
        let actual = vec![0.25, 0.50, 0.25];
        let expected = vec![0.250001, 0.499999, 0.25];

        let l1 = DifferentialTolerance::l1_distance(&actual, &expected).unwrap();
        assert!(l1 < 1e-4);

        assert!(DifferentialTolerance::assert_parity(&actual, &expected, 1e-4).is_ok());
        assert!(DifferentialTolerance::assert_parity(&actual, &expected, 1e-8).is_err());
    }

    #[test]
    fn test_nan_infinity_rejection() {
        assert!(DifferentialTolerance::assert_parity(&[f64::NAN], &[0.0], 1e-4).is_err());
        assert!(DifferentialTolerance::assert_parity(&[0.0], &[f64::INFINITY], 1e-4).is_err());
        assert!(DifferentialTolerance::l1_distance(&[f64::NAN], &[0.0]).is_err());
        assert!(DifferentialTolerance::l1_distance(&[0.0], &[f64::INFINITY]).is_err());
    }
}
