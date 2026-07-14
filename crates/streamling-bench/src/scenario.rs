//! Benchmark scenarios. Each drives `Kafka (Avro) → SQL transform → blackhole`
//! on a single-partition topic; only the SQL and its selectivity differ.

pub struct Scenario {
    pub name: &'static str,
    /// SQL for the transform. Must read `FROM kafka_source`.
    pub sql: &'static str,
    /// Fraction of source rows the transform emits. Sets the sink stop-count
    /// (`ceil(selectivity * records)`); the source still reads every row.
    pub selectivity: f64,
}

// The SELECT lists every payload column so projection pushdown does not prune
// them — the point is to exercise decode of the full record, not a thin slice.
pub const SCENARIOS: &[Scenario] = &[
    // Project + compute, drop no rows: input rows == output rows.
    Scenario {
        name: "avro_cdc_projection",
        sql: "SELECT id, user_id, email, lower(country), device, amount, amount * 1.1 AS amount_with_fee, price, ts FROM kafka_source",
        selectivity: 1.0,
    },
    // Same projection with a predicate that passes exactly 10% (see `sel_key`),
    // exercising predicate evaluation and a 10:1 source:sink work ratio.
    Scenario {
        name: "avro_cdc_filter",
        sql: "SELECT id, user_id, email, country, ts FROM kafka_source WHERE sel_key = 0",
        selectivity: 0.1,
    },
];
