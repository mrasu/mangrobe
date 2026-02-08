package mangrobe.flink.connector.record;

import mangrobe.api.Api;

import java.util.Collections;
import java.util.List;

public class MangrobeRecordBuilder {
    private final String tableName;
    private final long streamId;
    private final Api.Commit commit;

    public static MangrobeRecord build(String tableName, long streamId, Api.Commit commit) {
        var builder = new MangrobeRecordBuilder(tableName, streamId, commit);
        return builder.toRecord();
    }

    public MangrobeRecordBuilder(String tableName, long streamId, Api.Commit commit) {
        this.tableName = tableName;
        this.streamId = streamId;
        this.commit = commit;
    }

    private MangrobeRecord toRecord() {
        var commitId = commit.getCommitId();

        return switch (commit.getChangesCase()) {
            case ADDED_FILES ->
                    buildForAdded(tableName, streamId, commitId, toCommittedFiles(commit.getAddedFiles().getAddedFilesList()));
            case CHANGED_FILES -> {
                var changedFiles = new ChangedFiles(toCommittedFiles(commit.getChangedFiles().getDeletedFilesList()));
                yield buildForChanged(tableName, streamId, commitId, changedFiles);
            }
            case COMPACTED_FILES -> {
                var compactedFiles = toCompactedFiles(commit.getCompactedFiles().getCompactedFilesList());
                yield buildForCompacted(tableName, streamId, commitId, compactedFiles);
            }
            default -> buildForUnknown(tableName, streamId, commitId);
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
            String tableName,
            long streamId,
            String commitId,
            List<CommittedFile> addedFiles
    ) {
        return new MangrobeRecord(
                tableName, streamId, commitId,
                CommitChangeCase.ADDED_FILES,
                addedFiles, null, null
        );
    }

    private MangrobeRecord buildForChanged(
            String tableName,
            long streamId,
            String commitId,
            ChangedFiles changedFiles
    ) {
        return new MangrobeRecord(
                tableName, streamId, commitId,
                CommitChangeCase.CHANGED_FILES,
                null, changedFiles, null
        );
    }

    private MangrobeRecord buildForCompacted(
            String tableName,
            long streamId,
            String commitId,
            List<CompactedFile> compactedFiles
    ) {
        return new MangrobeRecord(
                tableName, streamId, commitId,
                CommitChangeCase.COMPACTED_FILES,
                null, null, compactedFiles
        );
    }

    private MangrobeRecord buildForUnknown(
            String tableName,
            long streamId,
            String commitId
    ) {
        return new MangrobeRecord(
                tableName, streamId, commitId,
                CommitChangeCase.UNKNOWN,
                null, null, null
        );
    }
}
