use arrow_array::RecordBatch;
use arrow_array::array::ArrayRef as ArrowArrayRef;
use arrow_array::builder::{
    ArrayBuilder, Float64Builder, Int64Builder, ListBuilder, StringBuilder, StructBuilder,
};
use arrow_schema::{DataType, Field, Fields};
use mangrobe_lab::prometheus_proto;
use mangrobe_lab::prometheus_proto::WriteRequest;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde::Serialize;
use std::io::Cursor;
use std::sync::Arc;

#[derive(Serialize)]
pub struct WriteRequestView {
    timeseries: Vec<TimeSeriesView>,
}

#[derive(Serialize)]
struct TimeSeriesView {
    labels: Vec<LabelView>,
    samples: Vec<SampleView>,
}

#[derive(Serialize)]
struct LabelView {
    name: String,
    value: String,
}

#[derive(Serialize)]
struct SampleView {
    value: f64,
    timestamp: i64,
}

impl From<&WriteRequest> for WriteRequestView {
    fn from(req: &WriteRequest) -> Self {
        Self {
            timeseries: req.timeseries.iter().map(TimeSeriesView::from).collect(),
        }
    }
}

impl From<&prometheus_proto::TimeSeries> for TimeSeriesView {
    fn from(ts: &prometheus_proto::TimeSeries) -> Self {
        Self {
            labels: ts.labels.iter().map(LabelView::from).collect(),
            samples: ts.samples.iter().map(SampleView::from).collect(),
        }
    }
}

impl From<&prometheus_proto::Label> for LabelView {
    fn from(label: &prometheus_proto::Label) -> Self {
        Self {
            name: label.name.clone(),
            value: label.value.clone(),
        }
    }
}

impl From<&prometheus_proto::Sample> for SampleView {
    fn from(sample: &prometheus_proto::Sample) -> Self {
        Self {
            value: sample.value,
            timestamp: sample.timestamp,
        }
    }
}

pub async fn create_parquet_from_write_request(
    buffer: &mut Vec<u8>,
    write_request: &WriteRequest,
) -> Result<(), anyhow::Error> {
    let mut ts_struct_builder = StructBuilder::from_fields(
        vec![
            Field::new(
                "labels",
                DataType::List(Arc::new(Field::new_list_field(
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
                DataType::List(Arc::new(Field::new_list_field(
                    DataType::Struct(Fields::from(vec![
                        Field::new("value", DataType::Float64, true),
                        Field::new("timestamp", DataType::Int64, true),
                    ])),
                    true,
                ))),
                true,
            ),
        ],
        0,
    );

    for ts in &write_request.timeseries {
        // labels
        let labels_list_builder = ts_struct_builder
            .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(0)
            .unwrap();
        let label_struct_builder = labels_list_builder
            .values()
            .as_any_mut()
            .downcast_mut::<StructBuilder>()
            .unwrap();

        for label in &ts.labels {
            label_struct_builder
                .field_builder::<StringBuilder>(0)
                .unwrap()
                .append_value(&label.name);
            label_struct_builder
                .field_builder::<StringBuilder>(1)
                .unwrap()
                .append_value(&label.value);
            label_struct_builder.append(true); // close label struct
        }
        labels_list_builder.append(true);

        // samples
        let samples_list_builder = ts_struct_builder
            .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(1)
            .unwrap();
        let sample_struct_builder = samples_list_builder
            .values()
            .as_any_mut()
            .downcast_mut::<StructBuilder>()
            .unwrap();

        for sample in ts.samples.iter() {
            sample_struct_builder
                .field_builder::<Float64Builder>(0)
                .unwrap()
                .append_value(sample.value);
            sample_struct_builder
                .field_builder::<Int64Builder>(1)
                .unwrap()
                .append_value(sample.timestamp);
            sample_struct_builder.append(true); // close label struct
        }
        samples_list_builder.append(true);

        ts_struct_builder.append(true); // close labels list for this timeseries
    }

    let batch = RecordBatch::try_from_iter(vec![(
        "timeseries",
        Arc::new(ts_struct_builder.finish()) as ArrowArrayRef,
    )])?;

    let mut cursor = Cursor::new(Vec::new());
    {
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(&mut cursor, batch.schema(), Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
    }
    *buffer = cursor.into_inner();

    Ok(())
}
