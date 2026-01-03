package mangrobe.flink.connector.record;

import java.util.List;
import java.util.Optional;

public class CompactedFile {
    private final List<CommittedFile> srcFiles;
    private final CommittedFile dstFile;

    public CompactedFile(List<CommittedFile> srcFiles, CommittedFile dstFile) {
        this.srcFiles = List.copyOf(srcFiles);
        this.dstFile = dstFile;
    }

    public List<CommittedFile> getSrcFiles() {
        return srcFiles;
    }

    public Optional<CommittedFile> getDstFile() {
        return Optional.ofNullable(dstFile);
    }
}
