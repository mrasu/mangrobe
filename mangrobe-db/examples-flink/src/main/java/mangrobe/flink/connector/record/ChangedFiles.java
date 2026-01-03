package mangrobe.flink.connector.record;

import java.util.List;

public class ChangedFiles {
    private final List<CommittedFile> deletedFiles;

    public ChangedFiles(List<CommittedFile> deletedFiles) {
        this.deletedFiles = deletedFiles;
    }
}
