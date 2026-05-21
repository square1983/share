package com.example.ibmcos.config;

import com.ibm.cloud.objectstorage.ClientConfiguration;
import com.ibm.cloud.objectstorage.auth.AWSCredentials;
import com.ibm.cloud.objectstorage.auth.AWSCredentialsProvider;
import com.ibm.cloud.objectstorage.auth.AWSStaticCredentialsProvider;
import com.ibm.cloud.objectstorage.auth.BasicAWSCredentials;
import com.ibm.cloud.objectstorage.auth.DefaultAWSCredentialsProviderChain;
import com.ibm.cloud.objectstorage.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.ibm.cloud.objectstorage.oauth.BasicIBMOAuthCredentials;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3ClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CosConfig {

    // IAM auth: set cos.auth-mode=iam and provide cos.api-key + cos.service-instance-id
    // HMAC auth: set cos.auth-mode=hmac and provide cos.access-key + cos.secret-key
    @Value("${cos.auth-mode:hmac}")
    private String authMode;

    @Value("${cos.api-key:}")
    private String apiKey;

    @Value("${cos.service-instance-id:}")
    private String serviceInstanceId;

    @Value("${cos.access-key:}")
    private String accessKey;

    @Value("${cos.secret-key:}")
    private String secretKey;

    @Value("${cos.endpoint}")
    private String endpoint;

    @Value("${cos.location}")
    private String location;

    @Bean
    public AmazonS3 cosClient() {
        AWSCredentialsProvider credentialsProvider = resolveCredentials();

        ClientConfiguration clientConfig = new ClientConfiguration()
                .withRequestTimeout(10_000)
                .withConnectionTimeout(5_000);
        clientConfig.setUseTcpKeepAlive(true);

        return AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withEndpointConfiguration(new EndpointConfiguration(endpoint, location))
                .withPathStyleAccessEnabled(true)
                .withClientConfiguration(clientConfig)
                .build();
    }

    private AWSCredentialsProvider resolveCredentials() {
        if ("iam".equalsIgnoreCase(authMode)) {
            AWSCredentials iamCreds = new BasicIBMOAuthCredentials(apiKey, serviceInstanceId);
            return new AWSStaticCredentialsProvider(iamCreds);
        }
        // HMAC: use explicit keys when provided, otherwise fall back to ~/.aws/credentials
        if (!accessKey.isBlank() && !secretKey.isBlank()) {
            return new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));
        }
        return DefaultAWSCredentialsProviderChain.getInstance();
    }
}
