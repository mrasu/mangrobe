package mangrobe.flink.connector.split;

import org.apache.flink.api.connector.source.SourceSplit;

import javax.annotation.Nullable;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class MangrobeSplit implements SourceSplit {
    private final long tableId;
    private final long streamId;
    @Nullable
    private final String startingCommitId;

    public MangrobeSplit(long tableId, long streamId, @Nullable String commitId) {
        this.tableId = tableId;
        this.streamId = streamId;
        this.startingCommitId = commitId;
    }

    public long getTableId() {
        return tableId;
    }

    public long getStreamId() {
        return streamId;
    }

    public Optional<String> getStartingCommitId() {
        return Optional.ofNullable(startingCommitId);
    }

    @Override
    public String splitId() {
        return this.tableId + ":" + this.streamId;
    }

    public byte[] serialize() {
        var commitId = this.startingCommitId == null ? "" : this.startingCommitId;
        var data = this.tableId + ":" + this.streamId + ":" + commitId;
        return data.getBytes(StandardCharsets.UTF_8);
    }

    public static MangrobeSplit deserialize(byte[] serialized) throws InvalidObjectException {
        var serializedText = new String(serialized, StandardCharsets.UTF_8);
        var splits = serializedText.split(":");
        if (!(splits.length == 2 || splits.length == 3)) {
            throw new InvalidObjectException("corrupted");
        }
        try {
            var tableId = Long.parseLong(splits[0]);
            var streamId = Long.parseLong(splits[1]);
            var commitId = splits.length == 3 ? splits[2] : null;
            return new MangrobeSplit(tableId, streamId, commitId);
        } catch (NumberFormatException e) {
            throw new InvalidObjectException("corrupted");
        }
    }
}
