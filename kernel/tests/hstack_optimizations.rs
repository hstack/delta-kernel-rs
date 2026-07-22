use std::path::PathBuf;
use tracing::info;
use buoyant_kernel::{DeltaResult, Snapshot};
use buoyant_kernel::scan::state::ScanFile;
use buoyant_kernel::scan::StatsOptions;

fn scan_metadata_callback(batches: &mut Vec<ScanFile>, scan_file: ScanFile) {
    batches.push(scan_file);
}

#[tokio::test]
async fn test_read_num_records() -> DeltaResult<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let path = "./tests/data/with_checkpoint";
    let path = std::fs::canonicalize(PathBuf::from(path))?;
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = test_utils::create_default_engine(&url)?;

    let snapshot = Snapshot::builder_for(url.clone()).build(engine.as_ref())?;
    let scan = snapshot
        .scan_builder()
        .with_stats(StatsOptions::none())
        .build()?;

    let scan_metadata = scan.scan_metadata(engine.as_ref())?;
    let mut scan_files = vec![];
    for res in scan_metadata {
        // info!("res: {:?}", res);
        let scan_metadata = res?;
        scan_files = scan_metadata.visit_scan_files(scan_files, scan_metadata_callback)?;
    }
    info!("SCAN FILES: {:?}", &scan_files);
    Ok(())

}
