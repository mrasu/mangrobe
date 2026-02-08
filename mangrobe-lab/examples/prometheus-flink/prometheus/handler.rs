use crate::prometheus::model::create_parquet_from_write_request;
use crate::{DEFAULT_PARTITION_TIME, PROM_STREAM_ID, PROM_TABLE_NAME};
use hyper::body::to_bytes;
use hyper::{Body, Request, Response, StatusCode};
use mangrobe_lab::ApiClient;
use mangrobe_lab::prometheus_proto::WriteRequest;
use mangrobe_lab::proto::{AddFileEntry, AddFileInfoEntry};
use object_store::aws::AmazonS3;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use prost::Message;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct Handler {
    s3: AmazonS3,
    api_client: ApiClient,
}

impl Handler {
    pub fn new(s3: AmazonS3, api_client: ApiClient) -> Self {
        Self { s3, api_client }
    }

    pub async fn handle_remote_write(
        &self,
        req: Request<Body>,
    ) -> Result<Response<Body>, anyhow::Error> {
        let (parts, body) = req.into_parts();
        let content_encoding = parts
            .headers
            .get(hyper::header::CONTENT_ENCODING)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("snappy");

        let raw_body = to_bytes(body).await?;
        let decompressed = match content_encoding {
            enc if enc.eq_ignore_ascii_case("snappy") => {
                snap::raw::Decoder::new().decompress_vec(&raw_body)?
            }
            other => {
                let mut resp = Response::new(Body::from(format!("unsupported encoding: {other}")));
                *resp.status_mut() = StatusCode::UNSUPPORTED_MEDIA_TYPE;
                return Ok(resp);
            }
        };

        let write_req = WriteRequest::decode(decompressed.as_slice())?;

        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
        let base_name = format!("prometheus-remote-write-{}", now.as_millis());
        let parquet_path = format!("prometheus/{base_name}.parquet");
        let mut buffer = Vec::<u8>::new();
        create_parquet_from_write_request(&mut buffer, &write_req).await?;

        let buffer_len = buffer.len();
        let payload = PutPayload::from_bytes(buffer.into());
        self.s3
            .put(&Path::from(parquet_path.clone()), payload)
            .await?;

        let add_file_entry = AddFileEntry {
            partition_time: Some(DEFAULT_PARTITION_TIME),
            file_info_entries: vec![AddFileInfoEntry {
                path: parquet_path.to_string(),
                size: buffer_len as i64,
            }],
        };

        self.api_client
            .add_files(
                PROM_TABLE_NAME.to_string(),
                PROM_STREAM_ID,
                vec![add_file_entry],
            )
            .await?;

        println!(
            "prometheus data is written: {} ({} bytes)",
            parquet_path, buffer_len
        );

        let mut resp = Response::new(Body::from("ok"));
        *resp.status_mut() = StatusCode::ACCEPTED;
        Ok(resp)
    }
}
