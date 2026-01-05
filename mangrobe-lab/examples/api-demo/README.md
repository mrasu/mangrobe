# API Demo

Run the gRPC API directly.

![](../../../docs/img/api_demo.png)

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
    cargo run
    ```
3. Run Object Storage(RustFS)
    ```shell
    cd mangrobe-lab
    docker compose up
    ```
4. Run api-demo
    ```shell
    cd mangrobe-lab
    cargo run --example api-demo
    ```

Then, you will see registered files are changed. like:
```
Running api-demo...

Adding files...
Run AddFiles! commit_id="1"
Current files: file1.txt, file2.txt, file3.txt, file4.txt

Compacting files...
Run AcquireFileLock! key=019b82e5-5104-7ee2-aacc-5866cac4f8fc, locked_file_count=2
Run CompactFiles! commit_id="2"
Current files: compacted.txt, file3.txt, file4.txt
...
```
