//! Checks of the [`TableBackend`] contract. Run them with [`run`] on a
//! multi-threaded Tokio runtime; a failed check panics.

use super::backend::{Mark, NamespaceSpec, TableBackend};
use crate::shutdown::ComponentScope;
use arrow::array::{Array, AsArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::common::ScalarValue;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

const NAMESPACE: &str = "rows";
const ID: &str = "id";
const NAME: &str = "name";
const TS: &str = "ts";
const NAME_INDEX: usize = 1;
/// How long a lookup gets to see a transaction's uncommitted writes.
const UNCOMMITTED_WINDOW: Duration = Duration::from_millis(200);

/// Opens the stores the checks run on.
#[async_trait]
pub trait BackendFactory: Send + Sync {
    /// A new, empty store. Each check uses its own `store` name.
    async fn create(&self, store: &str) -> Arc<dyn TableBackend>;
}

/// Runs every check.
pub async fn run(factory: &dyn BackendFactory) {
    namespaces_are_created_idempotently(factory).await;
    a_lookup_never_sees_a_partial_transaction(factory).await;
    reads_as_of_a_mark_undo_the_later_versions(factory).await;
    a_lookup_of_no_columns_reports_which_keys_exist(factory).await;
    trimming_raises_the_floor_and_not_trimming_keeps_it(factory).await;
    evictions_are_journaled_as_deletes(factory).await;
    a_transaction_reads_its_own_writes(factory).await;
    dropping_a_transaction_rolls_it_back(factory).await;
    meta_round_trips(factory).await;
    unversioned_namespaces_keep_no_journal(factory).await;
    a_paged_scan_visits_every_row_once(factory).await;
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(ID, DataType::Int64, false),
        Field::new(NAME, DataType::Utf8, true),
        Field::new(TS, DataType::Int64, true),
    ]))
}

fn spec(versioned: bool) -> NamespaceSpec {
    NamespaceSpec {
        name: NAMESPACE.to_string(),
        schema: schema(),
        key: vec![ID.to_string()],
        versioned,
        range_column: Some(TS.to_string()),
    }
}

/// `(id, name, ts)` rows.
fn rows(rows: &[(i64, &str, Option<i64>)]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int64Array::from_iter(rows.iter().map(|row| row.2))),
        ],
    )
    .expect("valid rows")
}

fn keys(ids: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(ID, DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(ids.to_vec()))],
    )
    .expect("valid keys")
}

async fn store(factory: &dyn BackendFactory, name: &str, versioned: bool) -> Arc<dyn TableBackend> {
    let backend = factory.create(name).await;
    backend.open().await.expect("open");
    backend
        .create_namespace(&spec(versioned))
        .await
        .expect("create the namespace");
    backend
}

/// Commits one transaction writing `upserts` and deleting `deletes`, and
/// returns its mark.
async fn apply(
    backend: &dyn TableBackend,
    upserts: &[(i64, &str, Option<i64>)],
    deletes: &[i64],
) -> Mark {
    let mut transaction = backend.begin().await.expect("begin");
    transaction
        .write(NAMESPACE, &rows(upserts), &keys(deletes))
        .await
        .expect("write");
    let version = transaction.version();
    transaction.commit().await.expect("commit");
    Mark::after(version)
}

/// Each id's name as of `mark`.
async fn names(backend: &dyn TableBackend, ids: &[i64], mark: Option<Mark>) -> Vec<Option<String>> {
    let found = backend
        .lookup(NAMESPACE, &keys(ids), &[NAME_INDEX], mark)
        .await
        .expect("lookup");
    assert_eq!(found.rows.num_rows(), ids.len(), "a row per key");
    let names = found.rows.column(0).as_string::<i32>();
    (0..ids.len())
        .map(|row| {
            assert_eq!(
                found.found.value(row),
                names.is_valid(row),
                "a found key has its name, a missing one doesn't"
            );
            names.is_valid(row).then(|| names.value(row).to_string())
        })
        .collect()
}

fn expected(names: &[Option<&str>]) -> Vec<Option<String>> {
    names.iter().map(|name| name.map(str::to_string)).collect()
}

fn ids(batch: &RecordBatch) -> Vec<i64> {
    batch
        .column(0)
        .as_primitive::<Int64Type>()
        .values()
        .to_vec()
}

async fn namespaces_are_created_idempotently(factory: &dyn BackendFactory) {
    let backend = store(factory, "conformance_namespaces", true).await;
    backend
        .create_namespace(&spec(true))
        .await
        .expect("creating an existing namespace again succeeds");
    let mut other = spec(true);
    other.schema = Arc::new(Schema::new(vec![Field::new(ID, DataType::Utf8, false)]));
    assert!(
        backend.create_namespace(&other).await.is_err(),
        "a namespace can't change its schema"
    );
    apply(backend.as_ref(), &[(1, "a", None)], &[]).await;
    backend.drop_namespace(NAMESPACE).await.expect("drop");
    backend.drop_namespace(NAMESPACE).await.expect("drop again");
    backend
        .create_namespace(&spec(true))
        .await
        .expect("recreate");
    assert_eq!(
        names(backend.as_ref(), &[1], None).await,
        [None],
        "dropped rows are gone"
    );
}

async fn a_lookup_never_sees_a_partial_transaction(factory: &dyn BackendFactory) {
    let backend = store(factory, "conformance_atomicity", true).await;
    apply(backend.as_ref(), &[(1, "a", None), (2, "b", None)], &[]).await;
    let mut transaction = backend.begin().await.expect("begin");
    transaction
        .write(NAMESPACE, &rows(&[(1, "a2", None)]), &keys(&[]))
        .await
        .expect("write");
    let reader = Arc::clone(&backend);
    let lookup = ComponentScope::detached("table conformance")
        .spawn(async move { names(reader.as_ref(), &[1, 2], None).await });
    tokio::time::sleep(UNCOMMITTED_WINDOW).await;
    transaction
        .write(NAMESPACE, &rows(&[(2, "b2", None)]), &keys(&[]))
        .await
        .expect("write");
    transaction.commit().await.expect("commit");
    let seen = lookup.await.expect("the lookup task");
    assert!(
        seen == expected(&[Some("a"), Some("b")]) || seen == expected(&[Some("a2"), Some("b2")]),
        "a lookup sees a transaction whole or not at all, got {seen:?}"
    );
    assert_eq!(
        names(backend.as_ref(), &[1, 2], None).await,
        expected(&[Some("a2"), Some("b2")])
    );
}

async fn reads_as_of_a_mark_undo_the_later_versions(factory: &dyn BackendFactory) {
    let backend = store(factory, "conformance_as_of", true).await;
    let backend = backend.as_ref();
    let start = Mark(backend.open().await.expect("open").next_version);
    let inserted = apply(backend, &[(1, "a", None)], &[]).await;
    let updated = apply(backend, &[(1, "b", None), (2, "x", None)], &[]).await;
    let deleted = apply(backend, &[], &[1]).await;
    let reinserted = apply(backend, &[(1, "c", None)], &[]).await;
    // Several changes of one key in one transaction.
    let mut transaction = backend.begin().await.expect("begin");
    for (upserts, deletes) in [
        (rows(&[(3, "p", None)]), keys(&[])),
        (rows(&[(3, "q", None)]), keys(&[])),
        (rows(&[]), keys(&[3])),
        (rows(&[(3, "r", None)]), keys(&[2])),
    ] {
        transaction
            .write(NAMESPACE, &upserts, &deletes)
            .await
            .expect("write");
    }
    let several = Mark::after(transaction.version());
    transaction.commit().await.expect("commit");

    for (mark, names_at) in [
        (Some(start), [None, None, None]),
        (Some(inserted), [Some("a"), None, None]),
        (Some(updated), [Some("b"), Some("x"), None]),
        (Some(deleted), [None, Some("x"), None]),
        (Some(reinserted), [Some("c"), Some("x"), None]),
        (Some(several), [Some("c"), None, Some("r")]),
        (Some(Mark(several.0 + 1)), [Some("c"), None, Some("r")]),
        (None, [Some("c"), None, Some("r")]),
    ] {
        assert_eq!(
            names(backend, &[1, 2, 3], mark).await,
            expected(&names_at),
            "as of {mark:?}"
        );
    }
    assert_eq!(
        names(backend, &[1], Some(updated)).await,
        expected(&[Some("b")]),
        "reads are repeatable"
    );
}

async fn a_lookup_of_no_columns_reports_which_keys_exist(factory: &dyn BackendFactory) {
    let backend = store(factory, "conformance_existence", true).await;
    let mark = apply(backend.as_ref(), &[(1, "a", None), (2, "b", None)], &[]).await;
    apply(backend.as_ref(), &[], &[1]).await;
    for (mark, expected) in [
        (Some(mark), [true, true, false]),
        (None, [false, true, false]),
    ] {
        let found = backend
            .lookup(NAMESPACE, &keys(&[1, 2, 3]), &[], mark)
            .await
            .expect("lookup");
        assert_eq!(
            (found.rows.num_columns(), found.rows.num_rows()),
            (0, 3),
            "no columns, a row per key"
        );
        assert_eq!(
            found.found.iter().collect::<Vec<_>>(),
            expected.map(Some),
            "as of {mark:?}"
        );
    }
}

async fn trimming_raises_the_floor_and_not_trimming_keeps_it(factory: &dyn BackendFactory) {
    let backend = store(factory, "conformance_floor", true).await;
    let backend = backend.as_ref();
    let first = apply(backend, &[(1, "a", None)], &[]).await;
    let second = apply(backend, &[(1, "b", None)], &[]).await;
    let third = apply(backend, &[(1, "c", None)], &[]).await;

    backend.finalize(third, false).await.expect("finalize");
    assert_eq!(
        names(backend, &[1], Some(first)).await,
        expected(&[Some("a")]),
        "finalizing without trimming keeps the floor"
    );

    backend.finalize(second, true).await.expect("finalize");
    assert!(
        backend
            .lookup(NAMESPACE, &keys(&[1]), &[NAME_INDEX], Some(first))
            .await
            .is_err(),
        "a read below the floor fails"
    );
    assert!(
        backend
            .lookup(NAMESPACE, &keys(&[]), &[NAME_INDEX], Some(first))
            .await
            .is_err(),
        "a read of no keys below the floor fails"
    );
    assert_eq!(
        names(backend, &[1], Some(second)).await,
        expected(&[Some("b")])
    );

    backend.finalize(first, true).await.expect("finalize");
    assert!(
        backend
            .lookup(NAMESPACE, &keys(&[1]), &[NAME_INDEX], Some(first))
            .await
            .is_err(),
        "finalizing never lowers the floor"
    );
    backend
        .finalize(second, true)
        .await
        .expect("finalize is idempotent");
    assert_eq!(
        names(backend, &[1], Some(second)).await,
        expected(&[Some("b")])
    );
    assert_eq!(
        names(backend, &[1], Some(third)).await,
        expected(&[Some("c")])
    );
}

async fn evictions_are_journaled_as_deletes(factory: &dyn BackendFactory) {
    let backend = store(factory, "conformance_eviction", true).await;
    let backend = backend.as_ref();
    let before = apply(
        backend,
        &[
            (1, "a", Some(1)),
            (2, "b", Some(5)),
            (3, "c", None),
            (4, "d", Some(3)),
        ],
        &[],
    )
    .await;
    let mut transaction = backend.begin().await.expect("begin");
    let evicted = transaction
        .evict_below(NAMESPACE, TS, &ScalarValue::Int64(Some(3)), false, 1)
        .await
        .expect("evict");
    assert_eq!(evicted.num_rows(), 1, "up to the limit");
    let evicted_more = transaction
        .evict_below(NAMESPACE, TS, &ScalarValue::Int64(Some(3)), false, 10)
        .await
        .expect("evict");
    let mut evicted_ids = ids(&evicted);
    evicted_ids.extend(ids(&evicted_more));
    evicted_ids.sort();
    assert_eq!(evicted_ids, [1, 3], "below the bound or null");
    let inclusive = transaction
        .evict_below(NAMESPACE, TS, &ScalarValue::Int64(Some(3)), true, 10)
        .await
        .expect("evict");
    assert_eq!(ids(&inclusive), [4], "equal to an inclusive bound");
    let after = Mark::after(transaction.version());
    transaction.commit().await.expect("commit");

    assert_eq!(
        names(backend, &[1, 2, 3, 4], Some(before)).await,
        expected(&[Some("a"), Some("b"), Some("c"), Some("d")])
    );
    assert_eq!(
        names(backend, &[1, 2, 3, 4], Some(after)).await,
        expected(&[None, Some("b"), None, None])
    );
}

async fn a_transaction_reads_its_own_writes(factory: &dyn BackendFactory) {
    let backend = store(factory, "conformance_own_writes", true).await;
    apply(backend.as_ref(), &[(1, "a", None), (2, "b", None)], &[]).await;
    let mut transaction = backend.begin().await.expect("begin");
    let replaced = transaction
        .write(
            NAMESPACE,
            &rows(&[(1, "z", None), (3, "new", None)]),
            &keys(&[2, 9]),
        )
        .await
        .expect("write");
    let mut replaced_ids = ids(&replaced);
    replaced_ids.sort();
    assert_eq!(
        replaced_ids,
        [1, 2],
        "the replaced and deleted rows, not the new ones"
    );
    let found = transaction
        .get(NAMESPACE, &keys(&[1, 2, 3, 9]))
        .await
        .expect("get");
    let names = found.rows.column(NAME_INDEX).as_string::<i32>();
    assert_eq!(
        (0..4)
            .map(|row| found.found.value(row).then(|| names.value(row)))
            .collect::<Vec<_>>(),
        [Some("z"), None, Some("new"), None]
    );
}

async fn dropping_a_transaction_rolls_it_back(factory: &dyn BackendFactory) {
    let backend = store(factory, "conformance_rollback", true).await;
    let backend = backend.as_ref();
    let mark = apply(backend, &[(1, "a", Some(1)), (2, "b", Some(9))], &[]).await;
    let mut transaction = backend.begin().await.expect("begin");
    transaction
        .write(
            NAMESPACE,
            &rows(&[(1, "x", None), (3, "c", None)]),
            &keys(&[2]),
        )
        .await
        .expect("write");
    transaction
        .write(NAMESPACE, &rows(&[(2, "y", None)]), &keys(&[1]))
        .await
        .expect("write");
    transaction
        .evict_below(NAMESPACE, TS, &ScalarValue::Int64(Some(10)), false, 10)
        .await
        .expect("evict");
    transaction
        .set_meta("rolled_back", b"x".to_vec())
        .await
        .expect("set meta");
    drop(transaction);

    assert_eq!(
        names(backend, &[1, 2, 3], None).await,
        expected(&[Some("a"), Some("b"), None])
    );
    assert!(
        !backend
            .open()
            .await
            .expect("open")
            .meta
            .contains_key("rolled_back"),
        "meta rolls back too"
    );
    let next = apply(backend, &[(3, "c", None)], &[1]).await;
    assert_eq!(
        names(backend, &[1, 2, 3], Some(mark)).await,
        expected(&[Some("a"), Some("b"), None])
    );
    assert_eq!(
        names(backend, &[1, 2, 3], Some(next)).await,
        expected(&[None, Some("b"), Some("c")])
    );
}

async fn meta_round_trips(factory: &dyn BackendFactory) {
    let backend = store(factory, "conformance_meta", true).await;
    let mut transaction = backend.begin().await.expect("begin");
    transaction.set_meta("a", b"1".to_vec()).await.expect("set");
    transaction.set_meta("a", b"2".to_vec()).await.expect("set");
    transaction.set_meta("b", Vec::new()).await.expect("set");
    transaction.commit().await.expect("commit");
    let meta = backend.open().await.expect("open").meta;
    assert_eq!(meta.get("a").map(Vec::as_slice), Some(b"2".as_slice()));
    assert_eq!(meta.get("b").map(Vec::as_slice), Some(b"".as_slice()));
}

async fn unversioned_namespaces_keep_no_journal(factory: &dyn BackendFactory) {
    let backend = store(factory, "conformance_unversioned", false).await;
    let backend = backend.as_ref();
    apply(backend, &[(1, "a", None)], &[]).await;
    apply(backend, &[(1, "b", None), (2, "c", None)], &[3]).await;
    assert_eq!(
        names(backend, &[1, 2], None).await,
        expected(&[Some("b"), Some("c")])
    );
    let stats = backend.stats().await.expect("stats");
    let stats = stats
        .iter()
        .find(|stats| stats.namespace == NAMESPACE)
        .expect("the namespace's stats");
    assert_eq!((stats.live_rows, stats.journal_rows), (2, 0));
}

async fn a_paged_scan_visits_every_row_once(factory: &dyn BackendFactory) {
    const ROWS: i64 = 25;
    const PAGE: usize = 7;
    let backend = store(factory, "conformance_scan", true).await;
    let backend = backend.as_ref();
    let inserts: Vec<(i64, &str, Option<i64>)> = (0..ROWS).map(|id| (id, "v", None)).collect();
    apply(backend, &inserts, &[]).await;
    apply(backend, &[(3, "u", None), (ROWS, "new", None)], &[4, 5]).await;
    let mut transaction = backend.begin().await.expect("begin");
    let mut seen = Vec::new();
    let mut cursor = None;
    loop {
        let (page, next) = transaction
            .scan(NAMESPACE, cursor, PAGE)
            .await
            .expect("scan");
        assert!(page.num_rows() <= PAGE, "a page holds at most the limit");
        seen.extend(ids(&page));
        match next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    let unique: BTreeSet<i64> = seen.iter().copied().collect();
    assert_eq!(seen.len(), unique.len(), "no row twice");
    assert_eq!(
        unique,
        (0..=ROWS).filter(|id| ![4, 5].contains(id)).collect(),
        "every live row"
    );
}
