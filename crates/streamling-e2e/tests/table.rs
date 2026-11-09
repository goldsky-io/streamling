//! `table` transform e2e tests, on the `in_memory` backend. The scenarios
//! live in `streamling_e2e::table_scenarios`, so a table backend plugin runs
//! them too.

use streamling_e2e::table_scenarios::{self, TableBackend};

#[tokio::test]
async fn test_table_output_to_postgres() {
    table_scenarios::table_output_to_postgres(&TableBackend::in_memory()).await;
}

#[tokio::test]
async fn test_table_without_readers_commits_offsets() {
    table_scenarios::table_without_readers_commits_offsets(&TableBackend::in_memory()).await;
}

#[tokio::test]
async fn test_lookup_eq_reads_the_table() {
    table_scenarios::lookup_eq_reads_the_table(&TableBackend::in_memory()).await;
}

#[tokio::test]
async fn test_lookups_read_a_composite_key_table() {
    table_scenarios::lookups_read_a_composite_key_table(&TableBackend::in_memory()).await;
}

#[tokio::test]
async fn test_table_window_evicts_rows_outside_it() {
    table_scenarios::table_window_evicts_rows_outside_it(&TableBackend::in_memory()).await;
}

#[tokio::test]
async fn test_table_agg_sliding_window() {
    table_scenarios::table_agg_sliding_window(&TableBackend::in_memory()).await;
}

#[tokio::test]
async fn test_table_watermark_window_backfill() {
    table_scenarios::table_watermark_window_backfill(&TableBackend::in_memory()).await;
}

#[tokio::test]
async fn test_table_read_by_two_consumers_at_once() {
    table_scenarios::table_read_by_two_consumers_at_once(&TableBackend::in_memory()).await;
}

#[tokio::test]
async fn test_lookup_eq_self_join_never_misses() {
    table_scenarios::lookup_eq_self_join_never_misses(&TableBackend::in_memory()).await;
}

#[tokio::test]
async fn test_lookup_eq_self_join_never_misses_across_partitions() {
    table_scenarios::lookup_eq_self_join_never_misses_across_partitions(&TableBackend::in_memory())
        .await;
}

#[tokio::test]
async fn test_table_agg_across_partitions() {
    table_scenarios::table_agg_across_partitions(&TableBackend::in_memory()).await;
}

#[tokio::test]
async fn test_table_reads_a_sql_transform() {
    table_scenarios::table_reads_a_sql_transform(&TableBackend::in_memory()).await;
}

#[tokio::test]
async fn test_table_over_a_filtered_source() {
    table_scenarios::table_over_a_filtered_source(&TableBackend::in_memory()).await;
}
