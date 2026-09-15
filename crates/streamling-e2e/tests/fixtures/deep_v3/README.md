These Parquet fixtures contain one row each: an Int64 primary key and `amount`.
The decimal fixtures all represent the exact number 1. Scale0 uses coefficient1;
scale2 uses coefficient100. Arbitrary decimals use LargeBinary and the documented
`streamling.decimal_arb` extension metadata with precision100. Native controls use
Decimal128(30,scale). Plain binary controls contain identical bytes without any
numeric annotation, and correctly serialize to hex.

The files were generated and independently read back with PyArrow21.0.0, preserving
embedded Arrow schema metadata. They use no compression, one row group, and dictionary
encoding. `manifest.json` describes each fixture. No Python dependency or external
repository is required at test runtime; Rust tests include these files directly.

The full-binary e2e target requires the usual isolated Kafka/Postgres test services.
It explicitly sets RUST_LOG=info because the print sink writes through tracing.
The pure common-crate format-option controls require no services.

The arbitrary-decimal e2e assertions cover support for the new type. They are not
claimed as before/after regressions in the existing Parquet extension contract:
the existing file reader defaults discard extension metadata. Their acceptable
outcomes are exact numeric preservation or an explicit rejection at source setup.
