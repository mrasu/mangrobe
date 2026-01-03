package mangrobe.flink.connector.record;

public class CommittedFile {
    private final String path;

    public CommittedFile(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }
}
