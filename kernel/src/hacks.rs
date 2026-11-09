use crate::schema::SchemaRef;
use crate::table_configuration::TableConfiguration;
use crate::{KernelResult, Snapshot};

pub fn new_kernel_table_configuration(
    input: &TableConfiguration,
    logical: &SchemaRef,
) -> KernelResult<TableConfiguration> {
    // not changed; callers must ensure that `logical` is a compatible interpretation of that schema.
    // @HStack FIXME: the order of the fields in the logical schema MIGHT BE DIFFERENT
    // we need to recompute the physical_schemas::full, which will in turn will recompute
    // physical_schemas::without_partitions
    TableConfiguration::try_new_with_schema(input, input.metadata.clone(), logical.clone())
}

pub fn new_kernel_snapshot(input: &Snapshot, table_configuration: TableConfiguration) -> Snapshot {
    input.with_table_configuration(table_configuration)
}
