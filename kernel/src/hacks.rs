use std::collections::HashSet;
use std::sync::{Arc, OnceLock};
use crate::schema::{SchemaRef, StructType};
use crate::{DeltaResult, Snapshot};
use crate::table_configuration::{PhysicalSchemas, TableConfiguration};

pub fn new_kernel_table_configuration(
    input: &TableConfiguration, logical: &SchemaRef) -> DeltaResult<TableConfiguration> {
    // @HStack FIXME: the order of the fields in the logical schema MIGHT BE DIFFERENT
    //   we need to recompute the physical_schemas::full, which will in turn will recompute
    //   physical_schemas::without_partitions
    let logical_schema = logical.clone();

    // copied from TableConfiguration::try_new_inner
    let physical_schema = Arc::new(logical_schema.make_physical(input.column_mapping_mode())?);

    let physical_schemas = PhysicalSchemas {
        full: physical_schema.clone(),
        // The without_partitions field is initialized, at read time from the OnceLock in
        // TableConfiguration::physical_data_schema_without_partition_columns
        // IF WE NEED TO - we can copy the code from there to initialize it eagerly
        without_partition: Arc::new(OnceLock::new()),
    };

    Ok(TableConfiguration {
        metadata: input.metadata.clone(),
        protocol: input.protocol.clone(),
        logical_schema: logical.clone(),
        physical_schemas: physical_schemas.clone(),
        table_properties: input.table_properties.clone(),
        column_mapping_mode: input.column_mapping_mode.clone(),
        table_root: input.table_root.clone(),
        version: input.version.clone(),
    })
}

pub fn new_kernel_snapshot(input: &Snapshot, table_configuration: TableConfiguration) -> Snapshot {
    Snapshot {
        span: input.span.clone(),
        log_segment: input.log_segment.clone(),
        table_configuration,
        crc: input.crc.clone(),
    }
}