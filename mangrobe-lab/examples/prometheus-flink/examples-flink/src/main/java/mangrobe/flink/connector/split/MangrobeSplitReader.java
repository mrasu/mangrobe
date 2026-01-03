package mangrobe.flink.connector.split;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import mangrobe.api.Api;
import mangrobe.api.DataManipulationServiceGrpc;
import mangrobe.flink.connector.record.*;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import java.io.IOException;
import java.util.ArrayList;

public class MangrobeSplitReader implements SplitReader<MangrobeRecord, MangrobeSplit> {
    private static final long NO_COMMIT_SLEEP_INTERVAL = 5_000;

    private final ArrayList<MangrobeSplitState> splitStates = new ArrayList<>();
    private final ManagedChannel channel;
    private int sleepCount = 0;

    public MangrobeSplitReader(String grpcTarget) {
        this.channel = ManagedChannelBuilder.forTarget(grpcTarget)
                .usePlaintext()
                .build();
    }

    @Override
    public RecordsWithSplitIds<MangrobeRecord> fetch() throws IOException {
        var recordsBySplitBuilder = new RecordsBySplits.Builder<MangrobeRecord>();

        for (var state : splitStates) {
            var now = System.currentTimeMillis();
            if (now < state.pollNextMillis) {
                continue;
            }

            var request = Api.GetCommitsRequest.newBuilder()
                    .setTableId(state.getTableId())
                    .setStreamId(state.getStreamId())
                    .setCommitIdAfter(state.getCurrentCommitId().orElse(""))
                    .build();
            var stub = DataManipulationServiceGrpc.newBlockingStub(this.channel);
            var response = stub.getCommits(request);

            var records = new ArrayList<MangrobeRecord>();
            for (var commit : response.getCommitsList()) {
                records.add(MangrobeRecordBuilder.build(state.getTableId(), state.getStreamId(), commit));
            }

            if (records.isEmpty()) {
                state.pollNextMillis = now + NO_COMMIT_SLEEP_INTERVAL;
                sleepCount++;
                if (sleepCount > 2) {
                    System.out.println("no change found. sleeping...");
                    sleepCount = 0;
                }
                continue;
            }
            sleepCount = 0;

            var lastCommitId = records.get(records.size() - 1).getCommitId();
            state.setCurrentCommitId(lastCommitId);
            recordsBySplitBuilder.addAll(state, records);
        }

        return recordsBySplitBuilder.build();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<MangrobeSplit> splitsChanges) {
        for (var split : splitsChanges.splits()) {
            splitStates.add(new MangrobeSplitState(split));
        }
    }

    @Override
    public void wakeUp() {
        // do nothing
    }

    @Override
    public void close() throws Exception {
        this.channel.shutdownNow();
    }

}
