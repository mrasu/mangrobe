package mangrobelabs.prometheus.util;

import org.apache.flink.api.common.SupportsConcurrentExecutionAttempts;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.io.IOException;

public class PromPrintSink<IN> implements Sink<IN>, SupportsConcurrentExecutionAttempts {
    public PromPrintSink() {
    }

    public SinkWriter<IN> createWriter(WriterInitContext context) throws IOException {
        return new PromPrintSinkWriter<>();
    }
}
