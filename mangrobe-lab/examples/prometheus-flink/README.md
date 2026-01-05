# Prometheus-Flink

Receive Prometheus metrics and consume them with Flink.

![](../../../docs/img/prometheus_flink.png)

# How to run

1. Run Docker Compose for api-server
    ```shell
    cd mangrobe-api-server
    docker compose up
    ```
2. Run api server
    ```shell
    cd mangrobe-api-server
    make migrate/fresh
    MANGROBE_API_ADDR=[::]:50051 cargo run
    ```
3. Run Object Storage(RustFS)
    ```shell
    cd mangrobe-lab
    docker compose up
    ```
4. Run Prometheus receiver
    ```shell
    cd mangrobe-lab
    cargo run --example prometheus-flink
    ```
5. Run Prometheus
    ```shell
    cd mangrobe-lab/examples/prometheus-flink
    docker compose --profile prometheus up
    ```
6. Run Flink
    ```shell
    cd mangrobe-lab/examples/prometheus-flink
    docker compose --profile flink up --build
    ```

Then, you will see Flink outputs text like: 
```
taskmanager-1  | [Mangrobe] Flink recognized Prometheus data: samples=[0.0], labels=[__name__=prometheus_remote_storage_samples_in_total,instance=prometheus:9090,job=prometheus]
taskmanager-1  | [Mangrobe] Flink recognized Prometheus data: samples=[46.0], labels=[__name__=prometheus_tsdb_wal_segment_current,instance=prometheus:9090,job=prometheus]
taskmanager-1  | [Mangrobe] Flink recognized Prometheus data: samples=[360448.0], labels=[__name__=prometheus_tsdb_wal_storage_size_bytes,instance=prometheus:9090,job=prometheus]
taskmanager-1  | [Mangrobe] Flink recognized Prometheus data: samples=[0.00570134], labels=[__name__=scrape_duration_seconds,instance=prometheus:9090,job=prometheus]
...
```
