package mangrobe.flink.connector.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

public class MangrobeSplitSerializer implements SimpleVersionedSerializer<MangrobeSplit> {
    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(MangrobeSplit obj) throws IOException {
        return obj.serialize();
    }

    @Override
    public MangrobeSplit deserialize(int version, byte[] serialized) throws IOException {
        return MangrobeSplit.deserialize(serialized);
    }
}
