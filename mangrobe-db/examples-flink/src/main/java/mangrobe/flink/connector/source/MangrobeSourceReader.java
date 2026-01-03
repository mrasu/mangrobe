package mangrobe.flink.connector.source;

import mangrobe.flink.connector.record.MangrobeRecord;
import mangrobe.flink.connector.split.MangrobeSplitReader;
import mangrobe.flink.connector.split.MangrobeSplit;
import mangrobe.flink.connector.split.MangrobeSplitState;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;

import java.util.Map;

public class MangrobeSourceReader extends SingleThreadMultiplexSourceReaderBase<MangrobeRecord, MangrobeRecord, MangrobeSplit, MangrobeSplitState> {
    private final SourceReaderContext context;

    public MangrobeSourceReader(RecordEmitter<MangrobeRecord, MangrobeRecord, MangrobeSplitState> recordEmitter,
                                Configuration config,
                                SourceReaderContext context,
                                String grpcTarget
    ) {
        super(() -> new MangrobeSplitReader(grpcTarget), recordEmitter, config, context);
        this.context = context;
    }

    @Override
    protected void onSplitFinished(Map<String, MangrobeSplitState> finishedSplitIds) {
        this.context.sendSplitRequest();
    }

    @Override
    protected MangrobeSplitState initializedState(MangrobeSplit split) {
        return new MangrobeSplitState(split);
    }

    @Override
    protected MangrobeSplit toSplitType(String splitId, MangrobeSplitState splitState) {
        return splitState.toMangrobeSplit();
    }
}
