package mangrobe.flink.connector.source;

import mangrobe.flink.connector.record.MangrobeRecord;
import mangrobe.flink.connector.record.MangrobeRecordEmitter;
import mangrobe.flink.connector.split.*;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;

public class MangrobeSource implements Source<MangrobeRecord, MangrobeSplit, String> {
    private final String grpcTarget;
    private final long tableId;


    public MangrobeSource(String grpcTarget, long tableId) {
        this.grpcTarget = grpcTarget;
        this.tableId = tableId;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<MangrobeSplit, String> createEnumerator(SplitEnumeratorContext<MangrobeSplit> enumContext) throws Exception {
        return new MangrobeSplitEnumerator(enumContext, this.grpcTarget, this.tableId);
    }

    @Override
    public SplitEnumerator<MangrobeSplit, String> restoreEnumerator(SplitEnumeratorContext<MangrobeSplit> enumContext, String checkpoint) throws Exception {
        return new MangrobeSplitEnumerator(enumContext, this.grpcTarget, this.tableId);
    }

    @Override
    public SimpleVersionedSerializer<MangrobeSplit> getSplitSerializer() {
        return new MangrobeSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<String> getEnumeratorCheckpointSerializer() {
        return new MangrobeSplitEnumeratorSerializer();
    }

    @Override
    public SourceReader<MangrobeRecord, MangrobeSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return new MangrobeSourceReader(new MangrobeRecordEmitter(), readerContext.getConfiguration(), readerContext, this.grpcTarget);
    }
}
