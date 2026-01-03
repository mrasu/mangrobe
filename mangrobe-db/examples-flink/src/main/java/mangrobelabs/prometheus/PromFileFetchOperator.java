package mangrobelabs.prometheus;

import mangrobe.flink.connector.record.CommitChangeCase;
import mangrobe.flink.connector.record.MangrobeRecord;
import mangrobelabs.prometheus.model.WriteRequest;
import mangrobelabs.prometheus.util.ArrowDumper;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.flink.api.common.functions.MapFunction;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.nio.file.Files.createTempFile;

public class PromFileFetchOperator implements MapFunction<MangrobeRecord, List<WriteRequest>> {
    private final String bucket;

    public PromFileFetchOperator(String bucket) {
        this.bucket = bucket;
    }

    @Override
    public List<WriteRequest> map(MangrobeRecord commit) throws Exception {
        if (commit.getChangeCase() != CommitChangeCase.ADDED_FILES) {
            return Collections.emptyList();
        }
        var addedFiles = commit.getAddedFiles();
        if (addedFiles == null) return Collections.emptyList();

        var writeRequests = new ArrayList<WriteRequest>();
        for (var file : addedFiles) {
            var key = file.getPath();
            var downloadPath = createTempFile("prometheus-remote-write-", ".parquet");

            try (var s3Client = S3.buildClient()) {
                var request = GetObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .build();
                var responseBytes = s3Client.getObjectAsBytes(request);
                java.nio.file.Files.write(downloadPath, responseBytes.asByteArray());

                var writeRequest = read(downloadPath);
                writeRequests.addAll(writeRequest);
            } finally {
                downloadPath.toFile().delete();
            }
        }

        return writeRequests;
    }

    private List<WriteRequest> read(Path path) throws Exception {
        try (var alloc = new RootAllocator();
             var datasetFactory = new FileSystemDatasetFactory(
                     alloc,
                     NativeMemoryPool.getDefault(),
                     FileFormat.PARQUET,
                     path.toUri().toString());
             var dataset = datasetFactory.finish();
             var scanner = dataset.newScan(new ScanOptions(1024));
             ArrowReader reader = scanner.scanBatches()) {
            var output = new ArrayList<WriteRequest>();
            while (reader.loadNextBatch()) {
                var root = reader.getVectorSchemaRoot();
                if (root.getRowCount() == 0) {
                    continue;
                }
                output.addAll(ArrowDumper.dumpTo(root, WriteRequest.class));
            }
            return output;
        }
    }
}
