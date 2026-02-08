package mangrobe.flink.connector.split;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import mangrobe.api.Api;
import mangrobe.api.InformationSchemaServiceGrpc;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.util.*;

public class MangrobeSplitEnumerator implements SplitEnumerator<MangrobeSplit, String> {
    private final SplitEnumeratorContext<MangrobeSplit> context;
    private final Queue<MangrobeSplit> pendingSplits = new ArrayDeque<>();
    private final List<Integer> readers = new ArrayList<>();
    private int nextReaderIndex = 0;
    private boolean isFirstFetch = true;

    private final ManagedChannel channel;
    private final String tableName;
    private final Set<Long> knownStreamIds = new HashSet<>();

    public MangrobeSplitEnumerator(SplitEnumeratorContext<MangrobeSplit> context, String grpcTarget, String tableName) {
        this.context = context;
        this.tableName = tableName;
        this.channel = ManagedChannelBuilder.forTarget(grpcTarget)
                .usePlaintext()
                .build();
    }

    @Override
    public void start() {
        context.callAsync(
                this::fetchNewSplits,
                this::handleNewSplits,
                0,
                3000
        );
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        this.assignToReaders();
    }

    @Override
    public void addSplitsBack(List<MangrobeSplit> splits, int subtaskId) {
        this.pendingSplits.addAll(splits);
        this.assignToReaders();
    }

    @Override
    public void addReader(int subtaskId) {
        if (!this.readers.contains(subtaskId)) {
            this.readers.add(subtaskId);
        }
        this.assignToReaders();
    }

    @Override
    public String snapshotState(long checkpointId) throws Exception {
        // Not implemented.
        return "";
    }

    @Override
    public void close() throws IOException {
        channel.shutdownNow();
    }

    private List<MangrobeSplit> fetchNewSplits() {
        var newSplits = new ArrayList<MangrobeSplit>();

        var nextToken = "";
        do {
            var request = Api.ListStreamsRequest.newBuilder()
                    .setPagination(
                            Api.PaginationRequest.newBuilder().setToken(nextToken).build())
                    .setTableName(this.tableName)
                    .build();

        var stub = InformationSchemaServiceGrpc.newBlockingStub(channel);
            var response = stub.listStreams(request);
            nextToken = response.getPagination().getNextToken();

            for (var stream : response.getStreamsList()) {
                var streamId = stream.getStreamId();
                if (knownStreamIds.add(streamId)) {
                    var commitId = isFirstFetch ? stream.getLastCommitId() : null;
                    newSplits.add(new MangrobeSplit(this.tableName, streamId, commitId));
                }
            }
        } while (!nextToken.isEmpty());

        isFirstFetch = false;
        return newSplits;
    }

    private void handleNewSplits(List<MangrobeSplit> splits, Throwable error) {
        if (error != null) {
            System.out.println("fetchNewSplits failed: " + error.getMessage());
            return;
        }
        this.pendingSplits.addAll(splits);
        this.assignToReaders();
    }

    private void assignToReaders() {
        if (pendingSplits.isEmpty() || readers.isEmpty()) {
            return;
        }
        Map<Integer, List<MangrobeSplit>> assignment = new HashMap<>();
        while (!pendingSplits.isEmpty()) {
            int reader = readers.get(nextReaderIndex);
            nextReaderIndex = (nextReaderIndex + 1) % readers.size();
            assignment.computeIfAbsent(reader, k -> new ArrayList<>()).add(pendingSplits.poll());
        }
        context.assignSplits(new SplitsAssignment<>(assignment));
    }
}
