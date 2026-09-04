//! Aggregation and report-only baseline comparison.

use std::cmp::Ordering;

use crate::BenchResult;

/// Median of a sample. Returns 0.0 for an empty slice.
pub fn median(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 1 {
        sorted[mid]
    } else {
        (sorted[mid - 1] + sorted[mid]) / 2.0
    }
}

/// One metric compared against its baseline value.
#[derive(Debug, Clone)]
pub struct MetricDelta {
    pub name: &'static str,
    pub current: f64,
    pub baseline: f64,
    pub delta_pct: f64,
    pub regressed: bool,
}

fn compare_metric(
    name: &'static str,
    current: f64,
    baseline: f64,
    threshold: f64,
    higher_is_better: bool,
) -> MetricDelta {
    let delta_pct = if baseline != 0.0 {
        (current - baseline) / baseline * 100.0
    } else {
        0.0
    };
    let regressed = if higher_is_better {
        current < baseline * (1.0 - threshold)
    } else {
        current > baseline * (1.0 + threshold)
    };
    MetricDelta {
        name,
        current,
        baseline,
        delta_pct,
        regressed,
    }
}

/// Compare the headline metrics against a baseline. Input throughput is
/// higher-is-better; per-record compute time is lower-is-better.
pub fn compare(current: &BenchResult, baseline: &BenchResult, threshold: f64) -> Vec<MetricDelta> {
    vec![
        compare_metric(
            "input_records_per_sec",
            current.throughput_input_records_per_sec_median,
            baseline.throughput_input_records_per_sec_median,
            threshold,
            true,
        ),
        compare_metric(
            "compute_us_per_input_record",
            current.compute_us_per_input_record_median,
            baseline.compute_us_per_input_record_median,
            threshold,
            false,
        ),
    ]
}

/// Print a human/markdown-friendly summary for one scenario. `comparison` is
/// empty when there is no baseline (or when seeding one).
pub fn print_report(result: &BenchResult, comparison: &[MetricDelta]) {
    println!("\n### {} ({})", result.scenario, result.format);
    let worker_threads = result
        .tokio_worker_threads
        .map(|count| count.to_string())
        .unwrap_or_else(|| "runtime-default".to_string());
    println!(
        "records={} payload={}B selectivity={} iterations={} tokio_workers={} runner={} version={}",
        result.records,
        result.payload_bytes,
        result.selectivity,
        result.iterations,
        worker_threads,
        result.runner_label,
        result.version,
    );
    println!(
        "  input throughput : {:.0} rec/s ({:.1} MB/s)",
        result.throughput_input_records_per_sec_median, result.throughput_mb_per_sec_median,
    );
    println!(
        "  output throughput: {:.0} rec/s",
        result.throughput_output_records_per_sec_median,
    );
    println!(
        "  compute          : {:.3} µs/record",
        result.compute_us_per_input_record_median,
    );
    println!(
        "  wall clock       : {:.2}s median, {:.2}s min",
        result.wall_clock_seconds_median, result.wall_clock_seconds_min,
    );
    println!(
        "  source rows seen : {} (expected ~{})",
        result.source_output_rows_observed, result.records,
    );

    if comparison.is_empty() {
        return;
    }
    println!("  vs baseline:");
    for delta in comparison {
        let flag = if delta.regressed {
            "⚠️ REGRESSED"
        } else {
            "ok"
        };
        println!(
            "    {:<28} {:>14.2} vs {:>14.2}  ({:+.1}%) {}",
            delta.name, delta.current, delta.baseline, delta.delta_pct, flag,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn median_odd_and_even() {
        assert_eq!(median(&[3.0, 1.0, 2.0]), 2.0);
        assert_eq!(median(&[4.0, 1.0, 3.0, 2.0]), 2.5);
    }

    #[test]
    fn median_empty_is_zero() {
        assert_eq!(median(&[]), 0.0);
    }

    #[test]
    fn throughput_drop_beyond_threshold_regresses() {
        // 20% drop, threshold 15% → regression.
        let delta = compare_metric("tps", 80.0, 100.0, 0.15, true);
        assert!(delta.regressed);
        assert!((delta.delta_pct - (-20.0)).abs() < 1e-9);
    }

    #[test]
    fn throughput_drop_within_threshold_is_ok() {
        // 10% drop, threshold 15% → not a regression.
        let delta = compare_metric("tps", 90.0, 100.0, 0.15, true);
        assert!(!delta.regressed);
    }

    #[test]
    fn throughput_improvement_is_never_regression() {
        let delta = compare_metric("tps", 130.0, 100.0, 0.15, true);
        assert!(!delta.regressed);
        assert!(delta.delta_pct > 0.0);
    }

    #[test]
    fn compute_time_rise_beyond_threshold_regresses() {
        // Lower-is-better: 20% rise, threshold 15% → regression.
        let delta = compare_metric("compute", 120.0, 100.0, 0.15, false);
        assert!(delta.regressed);
    }

    #[test]
    fn compute_time_drop_is_never_regression() {
        let delta = compare_metric("compute", 70.0, 100.0, 0.15, false);
        assert!(!delta.regressed);
    }
}
