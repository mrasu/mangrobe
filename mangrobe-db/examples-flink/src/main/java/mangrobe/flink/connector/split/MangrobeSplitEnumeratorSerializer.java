package mangrobe.flink.connector.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class MangrobeSplitEnumeratorSerializer implements SimpleVersionedSerializer<String> {
    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(String obj) throws IOException {
        return obj.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String deserialize(int version, byte[] serialized) throws IOException {
        return new String(serialized, StandardCharsets.UTF_8);
    }
}
