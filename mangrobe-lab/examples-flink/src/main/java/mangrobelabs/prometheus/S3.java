package mangrobelabs.prometheus;

import mangrobelabs.prometheus.util.Env;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.net.URI;

public class S3 {
    private static final String DEFAULT_ENDPOINT = "http://localhost:9000";
//    private static final URI ENDPOINT = URI.create("http://localhost:9000");

    public static S3Client buildClient() {
        var a = Env.getOr("MANGROBE_S3_ADDR", DEFAULT_ENDPOINT);
        var endpoint = URI.create(a);

        var credentials = StaticCredentialsProvider.create(
                AwsBasicCredentials.create("rustfsadmin", "rustfsadmin"));
        var s3Config = S3Configuration.builder()
                .pathStyleAccessEnabled(true)
                .build();

        return S3Client.builder()
                .endpointOverride(endpoint)
                .region(Region.US_EAST_1)
                .credentialsProvider(credentials)
                .serviceConfiguration(s3Config)
                .httpClient(UrlConnectionHttpClient.create())
                .build();
    }
}
