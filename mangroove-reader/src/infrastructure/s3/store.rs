use object_store::aws::AmazonS3;

// TODO: use env vars
pub fn create_rustfs() -> object_store::Result<AmazonS3> {
    object_store::aws::AmazonS3Builder::new()
        .with_endpoint("http://localhost:9000")
        .with_allow_http(true)
        .with_region("us-east-1")
        .with_bucket_name("mangroove-development")
        .with_access_key_id("rustfsadmin")
        .with_secret_access_key("rustfsadmin")
        .build()
}
