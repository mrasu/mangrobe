#[tokio::main]
async fn main() {
    mangrobe_api_server::migration::run_cli().await;
}
