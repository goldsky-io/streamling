#[derive(Clone, Debug)]
pub struct LagResult {
    pub metric_metadata_id: String,
    pub unit: String,
    pub head_at_provider: String,
    pub head_at: u64,
    pub tail_at: u64,
    pub tags: Vec<(String, String)>,
}
