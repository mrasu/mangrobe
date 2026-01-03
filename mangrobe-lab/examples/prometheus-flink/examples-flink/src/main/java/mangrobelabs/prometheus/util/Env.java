package mangrobelabs.prometheus.util;

public class Env {
    public static String getOr(String key, String def) {
        String value = System.getenv(key);
        return (value != null && !value.isEmpty()) ? value : def;
    }
}
