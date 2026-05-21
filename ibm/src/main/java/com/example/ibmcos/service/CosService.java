package com.example.ibmcos.service;

import com.ibm.cloud.objectstorage.services.s3.AmazonS3;
import com.ibm.cloud.objectstorage.services.s3.model.AmazonS3Exception;
import com.ibm.cloud.objectstorage.services.s3.model.ObjectMetadata;
import com.ibm.cloud.objectstorage.services.s3.model.PutObjectRequest;
import com.ibm.cloud.objectstorage.services.s3.model.PutObjectResult;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

@Service
public class CosService {

    public static final int MAX_ENBANGO = 3;

    public record PutResult(String usedKey, PutObjectResult s3Result) {}

    private final AmazonS3 cosClient;

    CosService(AmazonS3 cosClient) {
        this.cosClient = cosClient;
    }

    /** Upload without any conditional header — always overwrites. */
    public PutObjectResult put(String bucket, String key, InputStream data, long contentLength) {
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(contentLength);
        return cosClient.putObject(new PutObjectRequest(bucket, key, data, meta));
    }

    /** Upload with If-None-Match: * — throws AmazonS3Exception(412) if the object already exists. */
    public PutObjectResult putIfAbsent(String bucket, String key, InputStream data, long contentLength) {
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(contentLength);
        PutObjectRequest request = new PutObjectRequest(bucket, key, data, meta);
        request.putCustomRequestHeader("If-None-Match", "*");
        return cosClient.putObject(request);
    }

    /**
     * Upload with If-None-Match: *.
     * On 412 (key exists) retry with 枝番 suffix: _1, _2, _3 (MAX_ENBANGO times).
     * Returns the key that was actually used together with the S3 result.
     * Throws the last AmazonS3Exception when all candidates are exhausted.
     */
    public PutResult putWithEnbango(String bucket, String key, InputStream data, long contentLength) {
        byte[] bytes = readAllBytes(data);

        for (int attempt = 0; attempt <= MAX_ENBANGO; attempt++) {
            String candidate = attempt == 0 ? key : appendEnbango(key, attempt);
            try {
                PutObjectResult result = putIfAbsent(bucket, candidate,
                        new ByteArrayInputStream(bytes), bytes.length);
                return new PutResult(candidate, result);
            } catch (AmazonS3Exception e) {
                if (e.getStatusCode() != 412 || attempt == MAX_ENBANGO) {
                    throw e;
                }
                System.out.printf("[enbango] 412 on '%s', trying '%s'%n",
                        candidate, appendEnbango(key, attempt + 1));
            }
        }
        throw new IllegalStateException("unreachable");
    }

    // "dir/file.txt" + 2  →  "dir/file_2.txt"
    // "noext"        + 1  →  "noext_1"
    public static String appendEnbango(String key, int n) {
        int slash = key.lastIndexOf('/');
        int dot   = key.lastIndexOf('.');
        if (dot > slash + 1) {  // dot must not be the first char of the filename
            return key.substring(0, dot) + "_" + n + key.substring(dot);
        }
        return key + "_" + n;
    }

    private static byte[] readAllBytes(InputStream in) {
        try {
            return in.readAllBytes();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public boolean bucketExists(String bucket) {
        return cosClient.doesBucketExistV2(bucket);
    }

    public void createBucket(String bucket) {
        cosClient.createBucket(bucket);
    }

    public void deleteObject(String bucket, String key) {
        cosClient.deleteObject(bucket, key);
    }
}
