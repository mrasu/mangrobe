package mangrobe.flink.connector.record;

import mangrobe.api.Api;

import java.util.Collections;
import java.util.List;

public class MangrobeRecordBuilder {
    private final long tableId;
    private final long streamId;
    private final Api.Commit commit;

    public static MangrobeRecord build(long tableId, long streamId, Api.Commit commit) {
        var builder = new MangrobeRecordBuilder(tableId, streamId, commit);
        return builder.toRecord();
    }

    public MangrobeRecordBuilder(long tableId, long streamId, Api.Commit commit) {
        this.tableId = tableId;
        this.streamId = streamId;
        this.commit = commit;
    }

    private MangrobeRecord toRecord() {
        var commitId = commit.getCommitId();

        return switch (commit.getChangesCase()) {
            case ADDED_FILES ->
                    buildForAdded(tableId, streamId, commitId, toCommittedFiles(commit.getAddedFiles().getAddedFilesList()));
            case CHANGED_FILES -> {
                var changedFiles = new ChangedFiles(toCommittedFiles(commit.getChangedFiles().getDeletedFilesList()));
                yield buildForChanged(tableId, streamId, commitId, changedFiles);
            }
            case COMPACTED_FILES -> {
                var compactedFiles = toCompactedFiles(commit.getCompactedFiles().getCompactedFilesList());
                yield buildForCompacted(tableId, streamId, commitId, compactedFiles);
            }
            default -> buildForUnknown(tableId, streamId, commitId);
        };
    }

    private List<CommittedFile> toCommittedFiles(List<Api.CommittedFile> files) {
        return files.stream().map(file -> new CommittedFile(file.getPath())).toList();
    }

    private List<CompactedFile> toCompactedFiles(List<Api.CompactedFile> files) {
        if (files.isEmpty()) {
            return Collections.emptyList();
        }
        return files.stream().map(file -> {
            var srcFiles = toCommittedFiles(file.getSrcFilesList());
            var dstFile = new CommittedFile(file.getDstFile().getPath());
            return new CompactedFile(srcFiles, dstFile);
        }).toList();
    }

    private MangrobeRecord buildForAdded(
            long tableId,
            long streamId,
            String commitId,
            List<CommittedFile> addedFiles
    ) {
        return new MangrobeRecord(
                tableId, streamId, commitId,
                CommitChangeCase.ADDED_FILES,
                addedFiles, null, null
        );
    }

    private MangrobeRecord buildForChanged(
            long tableId,
            long streamId,
            String commitId,
            ChangedFiles changedFiles
    ) {
        return new MangrobeRecord(
                tableId, streamId, commitId,
                CommitChangeCase.CHANGED_FILES,
                null, changedFiles, null
        );
    }

    private MangrobeRecord buildForCompacted(
            long tableId,
            long streamId,
            String commitId,
            List<CompactedFile> compactedFiles
    ) {
        return new MangrobeRecord(
                tableId, streamId, commitId,
                CommitChangeCase.COMPACTED_FILES,
                null, null, compactedFiles
        );
    }

    private MangrobeRecord buildForUnknown(
            long tableId,
            long streamId,
            String commitId
    ) {
        return new MangrobeRecord(
                tableId, streamId, commitId,
                CommitChangeCase.UNKNOWN,
                null, null, null
        );
    }
}
