use std::collections::{BTreeSet, HashMap, HashSet};

use datafusion::prelude::SessionContext;

/// Return the set of Flink function names that are not present in DataFusion.
///
/// Name comparisons are case-insensitive and trimmed so that cosmetic
/// differences (whitespace, casing) do not produce false positives.
pub fn flink_only_functions<I1, S1, I2, S2>(
    flink_functions: I1,
    datafusion_functions: I2,
) -> BTreeSet<String>
where
    I1: IntoIterator<Item = S1>,
    S1: AsRef<str>,
    I2: IntoIterator<Item = S2>,
    S2: AsRef<str>,
{
    let flink_canonical = build_normalised_map(flink_functions);
    let datafusion_keys = build_normalised_key_set(datafusion_functions);

    flink_canonical
        .into_iter()
        .filter_map(|(normalised, canonical)| {
            if datafusion_keys.contains(&normalised) {
                None
            } else {
                Some(canonical)
            }
        })
        .collect()
}

fn build_normalised_map(
    names: impl IntoIterator<Item = impl AsRef<str>>,
) -> HashMap<String, String> {
    let mut entries = HashMap::new();

    for raw in names {
        let trimmed = raw.as_ref().trim();
        if trimmed.is_empty() {
            continue;
        }

        let canonical = trimmed.to_string();
        let normalised = canonical.to_ascii_lowercase();
        // Preserve the first casing we encountered for a given name
        entries.entry(normalised).or_insert(canonical);
    }

    entries
}

fn build_normalised_key_set(names: impl IntoIterator<Item = impl AsRef<str>>) -> HashSet<String> {
    names
        .into_iter()
        .map(|name| name.as_ref().trim().to_ascii_lowercase())
        .filter(|name| !name.is_empty())
        .collect()
}

/// Collect the names of DataFusion's built-in scalar functions.
///
/// Returned names are the canonical invocation spellings used by DataFusion.
pub fn datafusion_builtin_function_names() -> BTreeSet<String> {
    let ctx = SessionContext::new();

    ctx.state().scalar_functions().keys().cloned().collect()
}

/// Convenience helper that compares a list of Flink function names against
/// DataFusion's current built-in registry.
pub fn flink_builtin_functions_missing_from_datafusion<I, S>(flink_functions: I) -> BTreeSet<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let datafusion_names = datafusion_builtin_function_names();
    flink_only_functions(flink_functions, datafusion_names)
}
