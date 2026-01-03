package mangrobe.flink.connector.split;

import javax.annotation.Nullable;
import java.util.Optional;

public class MangrobeSplitState extends MangrobeSplit {
    @Nullable
    private String currentCommitId;
    long pollNextMillis;

    public MangrobeSplitState(MangrobeSplit split) {
        super(split.getTableId(), split.getStreamId(), split.getStartingCommitId().orElse(null));
        this.currentCommitId = split.getStartingCommitId().orElse(null);
        this.pollNextMillis = 0;
    }

    public Optional<String> getCurrentCommitId() {
        return Optional.ofNullable(currentCommitId);
    }

    public void setCurrentCommitId(@Nullable String commitId) {
        currentCommitId = commitId;
    }

    public MangrobeSplit toMangrobeSplit() {
        return new MangrobeSplit(getTableId(), getStreamId(), currentCommitId);
    }
}
