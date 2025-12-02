package infrastructure.s3.service;

import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class S3ReceivedService {
    private final S3Client s3Client;
    private final String bucketName;

    public S3ReceivedService(S3Client s3Client, String bucketName) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
    }

    public void processFiles(Path localFolder) {
        ListObjectsV2Request listRequest = ListObjectsV2Request
                .builder()
                .bucket(bucketName)
                .build();
        ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);

        List<S3Object> objects = listResponse.contents();

        for (int i = 0; i < objects.size(); i++) {
            S3Object object = objects.get(i);
            String key = object.key();
            String fileName = Paths.get(key).getFileName().toString();

            if (fileName.toUpperCase().contains("LIDO")) {
                System.out.println("Ignorando arquivo (já lido): " + fileName);
                continue;
            }

            Path localPath = localFolder.resolve(fileName);
            s3Client.getObject(GetObjectRequest.builder().bucket(bucketName).key(key).build(),
                    ResponseTransformer.toFile(localPath));

            String newKey = key.replace(".csv", "_LIDO.csv");
            s3Client.copyObject(CopyObjectRequest.builder()
                    .sourceBucket(bucketName)
                    .sourceKey(key)
                    .destinationBucket(bucketName)
                    .destinationKey(newKey)
                    .build());

            s3Client.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(key).build());
        }
    }
}