use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use super::super::{
    ScanMetadata, StatsOptions, StructStats, COMMIT_READ_SCHEMA,
    COMMIT_READ_SCHEMA_NO_JSON_STATS,
};
use crate::actions::{ADD_NAME, REMOVE_NAME};
use crate::arrow::array::{Array, Int64Array, StringArray, StructArray};
use crate::arrow::record_batch::RecordBatch;
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::sync::SyncEngine;
use crate::expressions::{ColumnName, Expression, Predicate};
use crate::schema::{DataType, SchemaRef};
use crate::{
    DeltaResult, DeltaResultIterator, DeltaResultIteratorStatic, Engine, EngineData,
    EvaluationHandler, FileDataReadResultIterator, FileMeta, FilteredEngineData, JsonHandler,
    ParquetFooter, ParquetHandler, PredicateRef, Snapshot, StorageHandler,
};

const RAW_CHECKPOINT_PATH: &str =
    "part-00000-a190be9e-e3df-439e-b366-06a863f51e99-c000.snappy.parquet";
const COMMIT_4_PATH: &str =
    "part-00000-40525115-50e1-4475-aae1-c8edc59274e6-c000.snappy.parquet";
const COMMIT_5_PATH: &str =
    "part-00000-c0cbdedc-d11b-4e4c-b6f2-5f40c55ef515-c000.snappy.parquet";
const COMMIT_4_STATS: &str = r#"{"numRecords":100,"minValues":{"id":401,"name":"name_401","age":20,"salary":90100,"ts_col":"1970-01-01T00:00:09.000Z"},"maxValues":{"id":500,"name":"name_500","age":69,"salary":100000,"ts_col":"1970-01-01T00:00:10.000Z"},"nullCount":{"id":0,"name":0,"age":0,"salary":0,"ts_col":0}}"#;
const COMMIT_5_STATS: &str = r#"{"numRecords":100,"minValues":{"id":501,"name":"name_501","age":20,"salary":100100,"ts_col":"1970-01-01T00:00:11.000Z"},"maxValues":{"id":600,"name":"name_600","age":69,"salary":110000,"ts_col":"1970-01-01T00:00:12.000Z"},"nullCount":{"id":0,"name":0,"age":0,"salary":0,"ts_col":0}}"#;
const PARSED_STATS_PATHS: [&str; 6] = [
    "part-00000-065eae2b-b4ea-4708-bb30-0888f35cabdd-c000.snappy.parquet",
    "part-00000-06d85a38-b141-479b-a315-4157335e9a11-c000.snappy.parquet",
    "part-00000-2d9663e0-37c0-425e-98df-2e7141f9b5fb-c000.snappy.parquet",
    COMMIT_4_PATH,
    "part-00000-a4c1def5-742e-4248-8c58-fc9f4018e43d-c000.snappy.parquet",
    COMMIT_5_PATH,
];

#[derive(Clone, Copy, Default)]
struct ForbiddenStats {
    raw: bool,
    parsed: bool,
}

#[derive(Clone)]
struct RequestedSchema {
    files: Vec<String>,
    schema: SchemaRef,
}

fn action_has_field(schema: &SchemaRef, action_name: &str, field_name: &str) -> bool {
    schema
        .field(action_name)
        .and_then(|field| match field.data_type() {
            DataType::Struct(action) => action.field(field_name),
            _ => None,
        })
        .is_some()
}

fn check_schema(schema: &SchemaRef, forbidden: ForbiddenStats, handler: &str) -> DeltaResult<()> {
    if forbidden.raw
        && [ADD_NAME, REMOVE_NAME]
            .iter()
            .any(|action| action_has_field(schema, action, "stats"))
    {
        return Err(crate::Error::generic(format!(
            "{handler} handler received forbidden raw stats schema"
        )));
    }
    if forbidden.parsed && action_has_field(schema, ADD_NAME, "stats_parsed") {
        return Err(crate::Error::generic(format!(
            "{handler} handler received forbidden parsed stats schema"
        )));
    }
    Ok(())
}

fn request(files: &[FileMeta], schema: SchemaRef) -> RequestedSchema {
    RequestedSchema {
        files: files
            .iter()
            .map(|file| file.location.path().to_string())
            .collect(),
        schema,
    }
}

struct SchemaCheckingJsonHandler {
    inner: Arc<dyn JsonHandler>,
    calls: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<RequestedSchema>>>,
    forbidden: ForbiddenStats,
}

impl JsonHandler for SchemaCheckingJsonHandler {
    fn parse_json(
        &self,
        json_strings: Box<dyn EngineData>,
        output_schema: SchemaRef,
    ) -> DeltaResult<Box<dyn EngineData>> {
        self.inner.parse_json(json_strings, output_schema)
    }

    fn read_json_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        if !files.is_empty() {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.requests
                .lock()
                .unwrap()
                .push(request(files, physical_schema.clone()));
        }
        check_schema(&physical_schema, self.forbidden, "JSON")?;
        self.inner
            .read_json_files(files, physical_schema, predicate)
    }

    fn write_json_file(
        &self,
        path: &url::Url,
        data: DeltaResultIterator<'_, FilteredEngineData>,
        overwrite: bool,
    ) -> DeltaResult<()> {
        self.inner.write_json_file(path, data, overwrite)
    }
}

struct SchemaCheckingParquetHandler {
    inner: Arc<dyn ParquetHandler>,
    calls: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<RequestedSchema>>>,
    forbidden: ForbiddenStats,
}

impl ParquetHandler for SchemaCheckingParquetHandler {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        if !files.is_empty() {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.requests
                .lock()
                .unwrap()
                .push(request(files, physical_schema.clone()));
        }
        check_schema(&physical_schema, self.forbidden, "Parquet")?;
        self.inner
            .read_parquet_files(files, physical_schema, predicate)
    }

    fn write_parquet_file(
        &self,
        location: url::Url,
        data: DeltaResultIteratorStatic<Box<dyn EngineData>>,
    ) -> DeltaResult<()> {
        self.inner.write_parquet_file(location, data)
    }

    fn read_parquet_footer(&self, file: &FileMeta) -> DeltaResult<ParquetFooter> {
        self.inner.read_parquet_footer(file)
    }
}

struct SchemaCheckingEngine {
    inner: Arc<SyncEngine>,
    json_handler: Arc<SchemaCheckingJsonHandler>,
    parquet_handler: Arc<SchemaCheckingParquetHandler>,
}

impl SchemaCheckingEngine {
    fn new(json_forbidden: ForbiddenStats, parquet_forbidden: ForbiddenStats) -> Self {
        let inner = Arc::new(SyncEngine::new());
        let json_handler = Arc::new(SchemaCheckingJsonHandler {
            inner: inner.json_handler(),
            calls: Arc::new(AtomicUsize::new(0)),
            requests: Arc::new(Mutex::new(vec![])),
            forbidden: json_forbidden,
        });
        let parquet_handler = Arc::new(SchemaCheckingParquetHandler {
            inner: inner.parquet_handler(),
            calls: Arc::new(AtomicUsize::new(0)),
            requests: Arc::new(Mutex::new(vec![])),
            forbidden: parquet_forbidden,
        });
        Self {
            inner,
            json_handler,
            parquet_handler,
        }
    }

    fn reset_requests(&self) {
        self.json_handler.calls.store(0, Ordering::Relaxed);
        self.parquet_handler.calls.store(0, Ordering::Relaxed);
        self.json_handler.requests.lock().unwrap().clear();
        self.parquet_handler.requests.lock().unwrap().clear();
    }

    fn json_calls(&self) -> usize {
        self.json_handler.calls.load(Ordering::Relaxed)
    }

    fn parquet_calls(&self) -> usize {
        self.parquet_handler.calls.load(Ordering::Relaxed)
    }

    fn json_requests(&self) -> Vec<RequestedSchema> {
        self.json_handler.requests.lock().unwrap().clone()
    }

    fn parquet_requests(&self) -> Vec<RequestedSchema> {
        self.parquet_handler.requests.lock().unwrap().clone()
    }
}

impl Engine for SchemaCheckingEngine {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        self.inner.evaluation_handler()
    }

    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.inner.storage_handler()
    }

    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.json_handler.clone()
    }

    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.parquet_handler.clone()
    }
}

fn snapshot_at(path: &str, engine: &dyn Engine) -> Arc<Snapshot> {
    let path = std::fs::canonicalize(PathBuf::from(path)).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    Snapshot::builder_for(url).build(engine).unwrap()
}

fn snapshot_at_version(path: &str, version: u64, engine: &dyn Engine) -> Arc<Snapshot> {
    let path = std::fs::canonicalize(PathBuf::from(path)).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    Snapshot::builder_for(url)
        .at_version(version)
        .build(engine)
        .unwrap()
}

fn assert_requests_stats(
    handler: &str,
    requests: &[RequestedSchema],
    raw: bool,
    parsed: bool,
) {
    assert!(!requests.is_empty(), "{handler} handler was not called");
    for request in requests {
        assert!(!request.files.is_empty(), "{handler} request had no files");
        assert_eq!(
            action_has_field(&request.schema, ADD_NAME, "stats"),
            raw,
            "unexpected add.stats projection for {handler} request {:?}",
            request.files
        );
        assert_eq!(
            action_has_field(&request.schema, ADD_NAME, "stats_parsed"),
            parsed,
            "unexpected add.stats_parsed projection for {handler} request {:?}",
            request.files
        );
    }
}

fn assert_request_extension(handler: &str, requests: &[RequestedSchema], extension: &str) {
    assert!(!requests.is_empty(), "{handler} handler was not called");
    assert!(
        requests
            .iter()
            .flat_map(|request| &request.files)
            .all(|file| file.ends_with(extension)),
        "{handler} request did not use only {extension} files"
    );
}

#[derive(Debug)]
struct MetadataRow {
    path: String,
    selected: bool,
    stats: Option<String>,
    stats_parsed_is_null: Option<bool>,
    num_records: Option<i64>,
    min_id: Option<i64>,
}

fn metadata_rows(batches: Vec<ScanMetadata>) -> Vec<MetadataRow> {
    let mut rows = Vec::new();
    for scan_metadata in batches {
        let (data, selection_vector) = scan_metadata.scan_files.into_parts();
        let batch: RecordBatch = ArrowEngineData::try_from_engine_data(data).unwrap().into();
        let paths = batch
            .column_by_name("path")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let stats = batch
            .column_by_name("stats")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let stats_parsed = batch
            .column_by_name("stats_parsed")
            .map(|array| array.as_any().downcast_ref::<StructArray>().unwrap());
        let num_records = stats_parsed.and_then(|parsed| {
            parsed
                .column_by_name("numRecords")
                .map(|array| array.as_any().downcast_ref::<Int64Array>().unwrap())
        });
        let min_id = stats_parsed
            .and_then(|parsed| parsed.column_by_name("minValues"))
            .map(|array| array.as_any().downcast_ref::<StructArray>().unwrap())
            .and_then(|min_values| min_values.column_by_name("id"))
            .map(|array| array.as_any().downcast_ref::<Int64Array>().unwrap());

        for row in 0..batch.num_rows() {
            if paths.is_null(row) {
                continue;
            }
            rows.push(MetadataRow {
                path: paths.value(row).to_string(),
                selected: selection_vector.get(row).copied().unwrap_or(true),
                stats: (!stats.is_null(row)).then(|| stats.value(row).to_string()),
                stats_parsed_is_null: stats_parsed.map(|parsed| parsed.is_null(row)),
                num_records: num_records
                    .filter(|values| !values.is_null(row))
                    .map(|values| values.value(row)),
                min_id: min_id
                    .filter(|values| !values.is_null(row))
                    .map(|values| values.value(row)),
            });
        }
    }
    rows
}

fn collect_metadata(scan: &super::super::Scan, engine: &dyn Engine) -> Vec<ScanMetadata> {
    scan.scan_metadata(engine)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn selected_paths(rows: &[MetadataRow]) -> Vec<String> {
    let mut paths: Vec<_> = rows
        .iter()
        .filter(|row| row.selected)
        .map(|row| row.path.clone())
        .collect();
    paths.sort();
    paths
}

fn expected_parsed_stats_paths() -> Vec<String> {
    let mut paths: Vec<_> = PARSED_STATS_PATHS
        .iter()
        .map(|path| path.to_string())
        .collect();
    paths.sort();
    paths
}

#[test]
fn existing_stats_options_enable_checkpoint_json_fallback() {
    let options = [
        StatsOptions::default(),
        StatsOptions::json_only(),
        StatsOptions::all_struct(),
        StatsOptions::struct_columns(vec![ColumnName::new(["id"])]),
        StatsOptions::all(),
        StatsOptions::none(),
    ];

    assert!(options
        .iter()
        .all(|option| option.checkpoint_stats_json_fallback));
}

#[test]
fn builder_disables_checkpoint_json_fallback_without_changing_projection() {
    let option = StatsOptions::struct_columns(vec![ColumnName::new(["id"])])
        .with_checkpoint_stats_json_fallback(false);

    assert!(!option.checkpoint_stats_json_fallback);
    assert!(!option.synthesize_json);
    assert!(matches!(option.struct_stats, StructStats::Columns(_)));
}

#[test]
fn stats_options_missing_fallback_field_defaults_true() {
    let mut serialized = serde_json::to_value(StatsOptions::all_struct()).unwrap();
    serialized
        .as_object_mut()
        .unwrap()
        .remove("checkpoint_stats_json_fallback");

    let deserialized: StatsOptions = serde_json::from_value(serialized).unwrap();
    assert!(deserialized.checkpoint_stats_json_fallback);
}

#[test]
fn no_stats_commit_schema_preserves_actions_but_omits_nested_stats() {
    for action_name in [ADD_NAME, REMOVE_NAME] {
        let expected: Vec<_> = match COMMIT_READ_SCHEMA.field(action_name).unwrap().data_type() {
            DataType::Struct(action) => action
                .fields()
                .filter(|field| field.name() != "stats")
                .map(|field| field.name().to_string())
                .collect(),
            _ => panic!("{action_name} must be a struct"),
        };
        let actual: Vec<_> = match COMMIT_READ_SCHEMA_NO_JSON_STATS
            .field(action_name)
            .unwrap()
            .data_type()
        {
            DataType::Struct(action) => action
                .fields()
                .map(|field| field.name().to_string())
                .collect(),
            _ => panic!("{action_name} must be a struct"),
        };

        assert_eq!(actual, expected);
        assert!(!action_has_field(
            &COMMIT_READ_SCHEMA_NO_JSON_STATS,
            action_name,
            "stats"
        ));
    }
}

#[test]
fn existing_typed_options_request_raw_only_checkpoint_stats_and_parse_them() {
    for (name, options) in [
        ("all_struct", StatsOptions::all_struct()),
        (
            "struct_columns",
            StatsOptions::struct_columns(vec![ColumnName::new(["int"])]),
        ),
    ] {
        let engine = SchemaCheckingEngine::new(
            ForbiddenStats {
                raw: false,
                parsed: true,
            },
            ForbiddenStats {
                raw: false,
                parsed: true,
            },
        );
        let checkpoint_snapshot = snapshot_at_version(
            "./tests/data/with_checkpoint_no_last_checkpoint/",
            2,
            &engine,
        );
        let latest_snapshot = snapshot_at(
            "./tests/data/with_checkpoint_no_last_checkpoint/",
            &engine,
        );
        engine.reset_requests();

        let checkpoint_scan = checkpoint_snapshot
            .scan_builder()
            .with_stats(options.clone())
            .build()
            .unwrap();
        let rows = metadata_rows(collect_metadata(&checkpoint_scan, &engine));
        let latest_scan = latest_snapshot
            .scan_builder()
            .with_stats(options)
            .build()
            .unwrap();
        let _ = collect_metadata(&latest_scan, &engine);

        assert!(engine.json_calls() > 0, "{name}: commit handler was not called");
        assert!(
            engine.parquet_calls() > 0,
            "{name}: checkpoint handler was not called"
        );
        let json_requests = engine.json_requests();
        let parquet_requests = engine.parquet_requests();
        assert_requests_stats("JSON", &json_requests, true, false);
        assert_requests_stats("Parquet", &parquet_requests, true, false);
        assert_request_extension("JSON", &json_requests, ".json");
        assert_request_extension("Parquet", &parquet_requests, ".parquet");

        assert_eq!(selected_paths(&rows), vec![RAW_CHECKPOINT_PATH]);
        let row = rows.iter().find(|row| row.path == RAW_CHECKPOINT_PATH).unwrap();
        assert_eq!(row.stats_parsed_is_null, Some(false), "{name}");
        assert_eq!(row.num_records, Some(5), "{name}");
    }
}

#[test]
fn fallback_disabled_raw_only_checkpoint_omits_stats_and_keeps_file_with_typed_null() {
    for (name, options) in [
        (
            "all_struct",
            StatsOptions::all_struct().with_checkpoint_stats_json_fallback(false),
        ),
        (
            "struct_columns",
            StatsOptions::struct_columns(vec![ColumnName::new(["int"])])
                .with_checkpoint_stats_json_fallback(false),
        ),
    ] {
        let engine = SchemaCheckingEngine::new(
            ForbiddenStats::default(),
            ForbiddenStats {
                raw: true,
                parsed: true,
            },
        );
        let snapshot = snapshot_at_version(
            "./tests/data/with_checkpoint_no_last_checkpoint/",
            2,
            &engine,
        );
        let latest_snapshot = snapshot_at(
            "./tests/data/with_checkpoint_no_last_checkpoint/",
            &engine,
        );
        engine.reset_requests();
        let predicate = Arc::new(Predicate::gt(
            Expression::column(["int"]),
            Expression::literal(10_000i64),
        ));

        let scan = snapshot
            .scan_builder()
            .with_predicate(predicate)
            .with_stats(options.clone())
            .build()
            .unwrap();
        let rows = metadata_rows(collect_metadata(&scan, &engine));
        let latest_scan = latest_snapshot
            .scan_builder()
            .with_stats(options)
            .build()
            .unwrap();
        let _ = collect_metadata(&latest_scan, &engine);

        assert!(engine.json_calls() > 0, "{name}: commit handler was not called");
        assert!(
            engine.parquet_calls() > 0,
            "{name}: checkpoint handler was not called"
        );
        let json_requests = engine.json_requests();
        let parquet_requests = engine.parquet_requests();
        assert_requests_stats("JSON", &json_requests, true, false);
        assert_requests_stats("Parquet", &parquet_requests, false, false);
        assert_request_extension("JSON", &json_requests, ".json");
        assert_request_extension("Parquet", &parquet_requests, ".parquet");

        assert_eq!(
            selected_paths(&rows),
            vec![RAW_CHECKPOINT_PATH],
            "{name}: missing stats must conservatively keep the checkpoint file"
        );
        let row = rows.iter().find(|row| row.path == RAW_CHECKPOINT_PATH).unwrap();
        assert_eq!(row.stats, None, "{name}");
        assert_eq!(row.stats_parsed_is_null, Some(true), "{name}");
        assert_eq!(row.num_records, None, "{name}");
    }
}

#[test]
fn compatible_checkpoint_and_newer_commits_use_their_own_stats_sources() {
    let engine = SchemaCheckingEngine::new(
        ForbiddenStats {
            raw: false,
            parsed: true,
        },
        ForbiddenStats {
            raw: true,
            parsed: false,
        },
    );
    let snapshot = snapshot_at("./tests/data/parsed-stats/", &engine);
    engine.reset_requests();

    let scan = snapshot
        .scan_builder()
        .with_stats(
            StatsOptions::struct_columns(vec![ColumnName::new(["id"])])
                .with_checkpoint_stats_json_fallback(false),
        )
        .build()
        .unwrap();
    let rows = metadata_rows(collect_metadata(&scan, &engine));

    assert!(engine.json_calls() > 0, "commit handler was not called");
    assert!(
        engine.parquet_calls() > 0,
        "checkpoint handler was not called"
    );
    let json_requests = engine.json_requests();
    let parquet_requests = engine.parquet_requests();
    assert_requests_stats("JSON", &json_requests, true, false);
    assert_requests_stats("Parquet", &parquet_requests, false, true);
    assert_request_extension("JSON", &json_requests, ".json");
    assert_request_extension("Parquet", &parquet_requests, ".parquet");

    assert_eq!(selected_paths(&rows), expected_parsed_stats_paths());
    for (path, expected_json, expected_min_id) in [
        (COMMIT_4_PATH, COMMIT_4_STATS, 401),
        (COMMIT_5_PATH, COMMIT_5_STATS, 501),
    ] {
        let row = rows.iter().find(|row| row.path == path).unwrap();
        assert!(row.selected);
        assert_eq!(row.stats.as_deref(), Some(expected_json));
        assert_eq!(row.stats_parsed_is_null, Some(false));
        assert_eq!(row.num_records, Some(100));
        assert_eq!(row.min_id, Some(expected_min_id));
    }

    for row in rows
        .iter()
        .filter(|row| row.path != COMMIT_4_PATH && row.path != COMMIT_5_PATH)
    {
        assert!(row.selected);
        assert_eq!(row.stats, None, "checkpoint raw JSON must not be projected");
        assert_eq!(row.stats_parsed_is_null, Some(false));
        assert_eq!(row.num_records, Some(100));
        assert!(row.min_id.is_some());
    }
}

#[test]
fn none_requests_no_stats_and_preserves_the_complete_active_file_set() {
    let forbidden = ForbiddenStats {
        raw: true,
        parsed: true,
    };
    let engine = SchemaCheckingEngine::new(forbidden, forbidden);
    let snapshot = snapshot_at("./tests/data/parsed-stats/", &engine);
    engine.reset_requests();

    let scan = snapshot
        .scan_builder()
        .with_stats(StatsOptions::none())
        .build()
        .unwrap();
    let rows = metadata_rows(collect_metadata(&scan, &engine));

    assert!(engine.json_calls() > 0, "commit handler was not called");
    assert!(
        engine.parquet_calls() > 0,
        "checkpoint handler was not called"
    );
    let json_requests = engine.json_requests();
    let parquet_requests = engine.parquet_requests();
    assert_requests_stats("JSON", &json_requests, false, false);
    assert_requests_stats("Parquet", &parquet_requests, false, false);
    assert_request_extension("JSON", &json_requests, ".json");
    assert_request_extension("Parquet", &parquet_requests, ".parquet");

    assert_eq!(selected_paths(&rows), expected_parsed_stats_paths());
    assert_eq!(rows.iter().filter(|row| row.selected).count(), 6);
    assert!(rows
        .iter()
        .filter(|row| row.selected)
        .all(|row| row.stats.is_none() && row.stats_parsed_is_null.is_none()));
}

#[test]
fn seeded_typed_stats_and_newer_json_commits_preserve_both_sources() {
    let seed_engine = SyncEngine::new();
    let seed_snapshot = snapshot_at_version("./tests/data/parsed-stats/", 3, &seed_engine);
    let seed_scan = seed_snapshot
        .scan_builder()
        .with_stats(
            StatsOptions::all_struct().with_checkpoint_stats_json_fallback(false),
        )
        .build()
        .unwrap();
    let seed_stats_schema = seed_scan.effective_replay_stats_schema().cloned();
    let seed_data: Vec<_> = seed_scan
        .scan_metadata(&seed_engine)
        .unwrap()
        .map(|result| result.and_then(|metadata| metadata.scan_files.apply_selection_vector()))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let engine = SchemaCheckingEngine::new(
        ForbiddenStats {
            raw: false,
            parsed: true,
        },
        ForbiddenStats::default(),
    );
    let snapshot = snapshot_at("./tests/data/parsed-stats/", &engine);
    let scan = snapshot
        .scan_builder()
        .with_stats(
            StatsOptions::all_struct().with_checkpoint_stats_json_fallback(false),
        )
        .build()
        .unwrap();
    engine.reset_requests();

    let batches = scan
        .scan_metadata_from(
            &engine,
            3,
            seed_stats_schema,
            seed_data.into_iter().map(Ok),
            None,
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let rows = metadata_rows(batches);

    assert!(engine.json_calls() > 0, "new commit handler was not called");
    assert_eq!(
        engine.parquet_calls(),
        0,
        "seeded replay must not reread the checkpoint"
    );
    let json_requests = engine.json_requests();
    assert_requests_stats("JSON", &json_requests, true, false);
    assert_request_extension("JSON", &json_requests, ".json");
    assert_eq!(selected_paths(&rows), expected_parsed_stats_paths());

    for (path, expected_json, expected_min_id) in [
        (COMMIT_4_PATH, COMMIT_4_STATS, 401),
        (COMMIT_5_PATH, COMMIT_5_STATS, 501),
    ] {
        let row = rows.iter().find(|row| row.path == path).unwrap();
        assert_eq!(row.stats.as_deref(), Some(expected_json));
        assert_eq!(row.stats_parsed_is_null, Some(false));
        assert_eq!(row.num_records, Some(100));
        assert_eq!(row.min_id, Some(expected_min_id));
    }
    assert!(rows
        .iter()
        .filter(|row| row.path != COMMIT_4_PATH && row.path != COMMIT_5_PATH)
        .all(|row| row.stats_parsed_is_null == Some(false) && row.num_records == Some(100)));
}
