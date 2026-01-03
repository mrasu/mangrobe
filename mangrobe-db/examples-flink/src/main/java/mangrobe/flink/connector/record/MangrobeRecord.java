package mangrobe.flink.connector.record;

import javax.annotation.Nullable;
import java.util.List;

public class MangrobeRecord {
    private final long tableId;
    private final long streamId;
    private final String commitId;
    private final CommitChangeCase changeCase;

    @Nullable
    private final List<CommittedFile> addedFiles;
    @Nullable
    private final ChangedFiles changedFiles;
    @Nullable
    private final List<CompactedFile> compactedFiles;

    MangrobeRecord(
            long tableId,
            long streamId,
            String commitId,
            CommitChangeCase changeCase,
            @Nullable List<CommittedFile> addedFiles,
            @Nullable ChangedFiles changedFiles,
            @Nullable List<CompactedFile> compactedFiles) {
        this.tableId = tableId;
        this.streamId = streamId;
        this.commitId = commitId;
        this.changeCase = changeCase == null ? CommitChangeCase.UNKNOWN : changeCase;
        this.addedFiles = addedFiles;
        this.changedFiles = changedFiles;
        this.compactedFiles = compactedFiles;
    }

    @Override
    public String toString() {
        return "tableId=" + tableId + ", streamId=" + streamId + ", commitId=" + commitId;
    }

    public String getCommitId() {
        return commitId;
    }

    public CommitChangeCase getChangeCase() {
        return changeCase;
    }

    public @Nullable List<CommittedFile> getAddedFiles() {
        return addedFiles;
    }

    public @Nullable ChangedFiles getChangedFiles() {
        return changedFiles;
    }

    public @Nullable List<CompactedFile> getCompactedFiles() {
        return compactedFiles;
    }
}
