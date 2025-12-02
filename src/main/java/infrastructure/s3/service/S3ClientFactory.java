package infrastructure.s3.service;

import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class S3ClientFactory {
    public static S3Client createClient(String acessKey, String secretKey, String sessionToken, Region region) {
        return S3Client.builder()
                .region(region)
                .credentialsProvider(
                        StaticCredentialsProvider.create(
                                AwsSessionCredentials.create(
                                        acessKey, secretKey, sessionToken
                                )
                        )
                )
                .build();
    }
}