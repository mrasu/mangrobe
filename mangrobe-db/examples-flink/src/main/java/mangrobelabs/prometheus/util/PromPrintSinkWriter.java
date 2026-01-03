package mangrobelabs.prometheus.util;

import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;
import java.io.Serializable;

public class PromPrintSinkWriter<IN> implements Serializable, SinkWriter<IN> {
    public PromPrintSinkWriter() {
    }

    public void write(IN record) {
        System.out.println("[Mangrobe] Flink recognized Prometheus data: " + record.toString());
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        write(element);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        System.out.flush();
    }

    @Override
    public void close() throws Exception {
    }
}
