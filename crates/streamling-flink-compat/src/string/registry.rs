use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::prelude::SessionContext;

use super::regex::{
    regexp_count_udf, regexp_extract_all_udf, regexp_extract_udf, regexp_instr_udf,
    regexp_replace_udf, regexp_substr_udf,
};
use super::scalars::{
    bin_udf, elt_udf, instr_udf, locate_udf, parse_url_udf, split_index_udf, split_udf,
    translate_udf, unhex_udf, url_decode_udf, url_encode_udf,
};

/// Register Flink-compatible aliases and string UDFs with the provided DataFusion
/// session.
///
/// The aliases cover naming differences (`charLength`, `dateFormat`, etc.), while
/// the UDFs re-implement behaviours that are present in Flink but absent in
/// DataFusion (for example `TRANSLATE`, `UNHEX`, or the full REGEXP suite).
pub fn register_string_aliases(ctx: &SessionContext) -> DataFusionResult<()> {
    static ALIASES: &[(&str, &[&str])] = &[
        ("starts_with", &["startswith"]),
        ("ends_with", &["endswith"]),
        ("left", &["LEFT"]),
        ("right", &["RIGHT"]),
        ("lpad", &["LPAD"]),
        ("rpad", &["RPAD"]),
        ("ltrim", &["LTRIM"]),
        ("rtrim", &["RTRIM"]),
        ("trim", &["TRIM"]),
        ("lower", &["LOWER", "lowercase"]),
        ("upper", &["UPPER", "uppercase"]),
        ("repeat", &["REPEAT"]),
        ("replace", &["REPLACE"]),
        ("reverse", &["REVERSE"]),
        ("split_part", &["SPLIT_PART"]),
        ("substring", &["SUBSTRING"]),
        ("substr", &["SUBSTR"]),
        ("regexp_like", &["regexp", "similar"]),
        ("coalesce", &["COALESCE"]),
        ("ascii", &["ASCII"]),
        ("btrim", &["BTRIM"]),
        (
            "character_length",
            &["CHAR_LENGTH", "charLength", "charlength"],
        ),
        ("chr", &["CHR"]),
        ("concat", &["CONCAT"]),
        ("concat_ws", &["CONCAT_WS"]),
        ("decode", &["DECODE"]),
        ("encode", &["ENCODE"]),
        ("initcap", &["initcap"]),
        ("overlay", &["OVERLAY"]),
        ("position", &["POSITION"]),
        ("to_char", &["DATE_FORMAT", "dateFormat"]),
        ("to_hex", &["hex"]),
        ("uuid", &["UUID"]),
    ];

    for (canonical, aliases) in ALIASES {
        register_alias(ctx, canonical, aliases)?;
    }

    ctx.register_udf(regexp_extract_udf());
    ctx.register_udf(regexp_extract_all_udf());
    ctx.register_udf(regexp_substr_udf());
    ctx.register_udf(regexp_count_udf());
    ctx.register_udf(regexp_instr_udf());
    ctx.register_udf(regexp_replace_udf());
    ctx.register_udf(locate_udf());
    ctx.register_udf(instr_udf());
    ctx.register_udf(bin_udf());
    ctx.register_udf(elt_udf());
    ctx.register_udf(parse_url_udf());
    ctx.register_udf(split_udf());
    ctx.register_udf(split_index_udf());
    ctx.register_udf(translate_udf());
    ctx.register_udf(unhex_udf());
    ctx.register_udf(url_encode_udf());
    ctx.register_udf(url_decode_udf());

    Ok(())
}

fn register_alias(
    ctx: &SessionContext,
    canonical_name: &str,
    aliases: &[&'static str],
) -> DataFusionResult<()> {
    let original = ctx
        .state()
        .scalar_functions()
        .get(canonical_name)
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "DataFusion built-in function '{canonical_name}' not found"
            ))
        })?
        .as_ref()
        .clone();

    ctx.register_udf(original.with_aliases(aliases.iter().copied()));
    Ok(())
}
