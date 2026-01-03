use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, SharedCredentialsProvider};
use object_store::aws::AmazonS3;

pub fn create_rustfs(bucket_name: String) -> object_store::Result<AmazonS3> {
    object_store::aws::AmazonS3Builder::new()
        .with_endpoint("http://localhost:9000")
        .with_allow_http(true)
        .with_region("us-east-1")
        .with_bucket_name(bucket_name)
        .with_access_key_id("rustfsadmin")
        .with_secret_access_key("rustfsadmin")
        .build()
}

pub async fn create_bucket_if_not_exists(bucket_name: String) -> Result<(), anyhow::Error> {
    let config = SdkConfig::builder()
        .behavior_version(BehaviorVersion::latest())
        .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
            "rustfsadmin",
            "rustfsadmin",
            None,
            None,
            "mangrobe",
        )))
        .region(Region::new("us-east-1"))
        .endpoint_url("http://localhost:9000")
        .build();

    let s3_config = aws_sdk_s3::config::Builder::from(&config)
        .force_path_style(true)
        .build();
    let client = Client::from_conf(s3_config);

    let result = client
        .head_bucket()
        .bucket(bucket_name.clone())
        .send()
        .await;
    if result.is_ok() {
        return Ok(());
    }

    client.create_bucket().bucket(bucket_name).send().await?;
    Ok(())
}
