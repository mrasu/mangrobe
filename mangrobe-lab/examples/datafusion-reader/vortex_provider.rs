use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use mangrobe_lab::{ApiClient, Stream};
use std::any::Any;
use std::sync::Arc;
use vortex::VortexSessionDefault;
use vortex::session::VortexSession;
use vortex_datafusion::VortexFormat;

#[derive(Debug)]
pub struct VortexProvider {
    api_client: ApiClient,
    object_store_url: ObjectStoreUrl,
    format: VortexFormat,
    stream: Stream,
}

#[async_trait]
impl TableProvider for VortexProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        SchemaRef::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("code", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let response = self
            .api_client
            .fetch_current_state(self.stream.table_name.clone(), self.stream.stream_id)
            .await
            .map_err(|e| DataFusionError::External(e.into()))?;
        let files: Vec<_> = response
            .get_ref()
            .files
            .iter()
            .map(|f| PartitionedFile::new(f.path.clone(), f.size as u64))
            .collect();

        let scan_config = FileScanConfigBuilder::new(
            self.object_store_url.clone(),
            self.schema(),
            self.format.file_source(),
        )
        .with_file_group(FileGroup::new(files))
        .build();
        self.format.create_physical_plan(state, scan_config).await
    }
}

impl VortexProvider {
    pub(crate) fn new(
        api_client: ApiClient,
        object_store_url: &ObjectStoreUrl,
        stream: Stream,
    ) -> Result<Self> {
        let format = VortexFormat::new(VortexSession::default());
        Ok(Self {
            api_client,
            object_store_url: object_store_url.clone(),
            format,
            stream,
        })
    }
}
