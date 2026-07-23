use crate::schema::SchemaRef;
use crate::{DeltaResult, Snapshot};
use crate::table_configuration::TableConfiguration;

pub fn new_kernel_table_configuration(
    input: &TableConfiguration, logical: &SchemaRef) -> DeltaResult<TableConfiguration> {
    // @HStack FIXME: the order of the fields in the logical schema MIGHT BE DIFFERENT
    //   we need to recompute the physical_schemas::full, which will in turn will recompute
    //   physical_schemas::without_partitions

    let metadata = input.metadata.clone();
    let protocol = input.protocol.clone();
    let table_root = input.table_root.clone();
    let version = input.version;

    TableConfiguration::try_new_inner(metadata, protocol, table_root, version, logical.clone())
}

pub fn new_kernel_snapshot(input: &Snapshot, table_configuration: TableConfiguration) -> Snapshot {
    Snapshot {
        span: input.span.clone(),
        log_segment: input.log_segment.clone(),
        table_configuration,
        crc: input.crc.clone(),
    }
}