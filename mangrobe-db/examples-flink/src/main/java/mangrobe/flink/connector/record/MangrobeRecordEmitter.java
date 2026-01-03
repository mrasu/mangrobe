package mangrobe.flink.connector.record;

import mangrobe.flink.connector.split.MangrobeSplitState;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

public class MangrobeRecordEmitter implements RecordEmitter<MangrobeRecord, MangrobeRecord, MangrobeSplitState> {
    public MangrobeRecordEmitter() {
    }

    @Override
    public void emitRecord(MangrobeRecord commit, SourceOutput<MangrobeRecord> output, MangrobeSplitState splitState) throws Exception {
        output.collect(commit);
    }
}
