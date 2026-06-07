pub fn metric_metadata_id_to_reference_name(metric_metadata_id: &str) -> Option<String> {
    metric_metadata_id
        .split("::")
        .collect::<Vec<&str>>()
        .last()
        .map(|s| s.to_string())
}
