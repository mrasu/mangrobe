package mangrobelabs.prometheus;

import mangrobe.flink.connector.record.MangrobeRecord;
import mangrobe.flink.connector.source.MangrobeSource;
import mangrobelabs.prometheus.model.Timeseries;
import mangrobelabs.prometheus.model.WriteRequest;
import mangrobelabs.prometheus.util.Env;
import mangrobelabs.prometheus.util.PromPrintSink;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Set;


public class Main {
    private static final Long PROM_TABLE_ID = 901L;
    private static final Set<String> TARGET_LABELS = Set.of(
            "prometheus_tsdb_wal_storage_size_bytes", "prometheus_tsdb_wal_segment_current",
            "prometheus_remote_storage_samples_in_total", "scrape_duration_seconds"
    );

    private static final String DEFAULT_MANGROBE_API_ADDR = "dns:///localhost:50051";

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Flink Prometheus reader using Mangrobe...");
        readPrometheusData(Env.getOr("MANGROBE_API_ADDR", DEFAULT_MANGROBE_API_ADDR));
    }

    private static void readPrometheusData(String grpcTarget) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        DataStream<MangrobeRecord> records = env
                .fromSource(new MangrobeSource(grpcTarget, PROM_TABLE_ID), WatermarkStrategy.noWatermarks(), "mangrobe-examples-flink");

        var promWriteRequests = records.map(new PromFileFetchOperator("mangrobe-development"));

        var results = promWriteRequests.flatMap((List<WriteRequest> requests, Collector<Timeseries> output) -> {
            for (var req : requests) {
                for (var ts : req.timeseries) {
                    for (var label : ts.labels) {
                        if (!label.name.equals("__name__")) continue;

                        if (TARGET_LABELS.contains(label.value)) {
                            output.collect(ts);
                        }
                        break;
                    }
                }
            }
        }).returns(Types.POJO(Timeseries.class));

        results.sinkTo(new PromPrintSink<>());

        env.execute("Mangrobe Prometheus Reader");
    }
}
