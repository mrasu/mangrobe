package mangrobelabs.prometheus.model;

import java.util.List;
import java.util.stream.Collectors;

public class Timeseries {
    public List<Label> labels;
    public List<Sample> samples;

    public Timeseries() {
    }

    @Override
    public String toString() {
        var sampleText = samples.stream()
                .map(sample -> String.valueOf(sample.value))
                .collect(Collectors.joining(","));

        var labelText = labels.stream()
                .map(label -> label.name + "=" + label.value)
                .collect(Collectors.joining(","));

        return "samples=[" + sampleText + "], labels=[" + labelText + "]";
    }
}
