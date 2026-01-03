use crate::grpc::api_client::ApiClient;
use crate::prometheus::vortex::{PROM_STREAM_ID, PROM_TABLE_ID};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::DataFusionError;
use datafusion::datasource::TableType;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use std::any::Any;
use std::sync::Arc;
use vortex::VortexSessionDefault;
use vortex::session::VortexSession;
use vortex_datafusion::VortexFormat;

#[derive(Debug)]
pub struct Provider {
    api_client: ApiClient,
    object_store_url: ObjectStoreUrl,
    format: VortexFormat,
}

#[async_trait]
impl TableProvider for Provider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        SchemaRef::new(Schema::new(vec![Field::new(
            "timeseries",
            DataType::Struct(Fields::from(vec![
                Field::new(
                    "labels",
                    DataType::List(Arc::new(Field::new(
                        "item",
                        DataType::Struct(Fields::from(vec![
                            Field::new("name", DataType::Utf8, true),
                            Field::new("value", DataType::Utf8, true),
                        ])),
                        true,
                    ))),
                    true,
                ),
                Field::new(
                    "samples",
                    DataType::List(Arc::new(Field::new(
                        "item",
                        DataType::Struct(Fields::from(vec![
                            Field::new("value", DataType::Float64, true),
                            Field::new("timestamp", DataType::Int64, true),
                        ])),
                        true,
                    ))),
                    true,
                ),
            ])),
            true,
        )]))
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
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let response = self
            .api_client
            .fetch_snapshot(PROM_TABLE_ID, PROM_STREAM_ID)
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

impl Provider {
    pub(crate) fn new(
        api_client: ApiClient,
        object_store_url: &ObjectStoreUrl,
    ) -> datafusion::common::Result<Self> {
        let format = VortexFormat::new(VortexSession::default());
        Ok(Self {
            api_client,
            object_store_url: object_store_url.clone(),
            format,
        })
    }
}
