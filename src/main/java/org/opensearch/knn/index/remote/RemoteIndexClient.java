/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.remote;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.knn.index.KNNSettings;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.utils.StringInputStream;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Log4j2
public class RemoteIndexClient {

    private static RemoteIndexClient INSTANCE;
    private volatile SdkHttpClient httpClient;

    RemoteIndexClient() {
        this.httpClient = createHttpClient();
    }

    public static synchronized RemoteIndexClient getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new RemoteIndexClient();
        }
        return INSTANCE;
    }

    private SdkHttpClient createHttpClient() {
        // HttpHost host = new HttpHost(String.valueOf(KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_ENDPOINT)));
        HttpHost host = new HttpHost("localhost", 8888, "http");
        AuthScope authScope = new AuthScope(host);
        BasicCredentialsProvider basicCredentialsProvider = new BasicCredentialsProvider();
        basicCredentialsProvider.setCredentials(authScope, new UsernamePasswordCredentials("demo", "demo"));
        return ApacheHttpClient.builder().credentialsProvider(basicCredentialsProvider).build();

    }

    public String submitVectorBuild() throws IOException, URISyntaxException {
        HttpExecuteRequest buildRequest = constructBuildRequest();
        HttpExecuteResponse response = httpClient.prepareRequest(buildRequest).call();
        String responseBody = response.responseBody().map(body -> {
            try {
                return new String(body.readAllBytes(), StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new RuntimeException("Failed to read response body", e);
            }
        }).orElse("Empty response");

        // Print response details
        System.out.println("Response Status: " + response.httpResponse().statusCode());
        System.out.println("Response Body: " + responseBody);

        if (response.httpResponse().statusCode() != 200) {
            throw new RuntimeException("Failed to submit vector build: " + responseBody);
        }

        String jobID = null;
        if (response.httpResponse().isSuccessful()) {
            jobID = response.responseBody().toString();
        }
        return jobID;
    }

    @SuppressWarnings("BusyWait")
    public String awaitVectorBuild(String jobId) throws InterruptedException, IOException, URISyntaxException {
        long startTime = System.currentTimeMillis();
        long timeout = ((TimeValue) (KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_TIMEOUT))).getMinutes();
        long pollInterval = KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_POLL_INTERVAL);

        Thread.sleep(1000 * 60); // TODO Set initial delay

        while (System.currentTimeMillis() - startTime < timeout) {
            HttpExecuteResponse statusResponse = getBuildStatus(jobId);
            String status = statusResponse.responseBody().get().toString();

            // TODO Return full response for error message in FAILED and build path in COMPLETED

            switch (status) {
                case "COMPLETED_INDEX_BUILD":
                    return jobId; // return path from response
                case "FAILED_INDEX_BUILD":
                    throw new RuntimeException("Index build failed");
                // return error message
                case "RUNNING_INDEX_BUILD":
                    Thread.sleep(pollInterval);
            }
        }
        try {
            cancelBuild(jobId);
        } catch (Exception e) {
            log.error("Failed to cancel build after timeout", e);
        }
        return null; // TODO: Fallback to local CPU
    }

    // TODO maybe more maintainable if this takes in a BuildRequest object that we can tweak in the future.
    public HttpExecuteRequest constructBuildRequest() throws MalformedURLException, URISyntaxException, JsonProcessingException {
        // URI endpoint = new URL(KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_ENDPOINT) + "/_build").toURI();
        URI endpoint = new URL("http://localhost:8888" + "/_build").toURI();

        Map<String, Object> methodParams = new HashMap<>();
        methodParams.put("ef_construction", 128);
        methodParams.put("m", 16);

        RemoteBuildRequest remoteBuildRequest = new RemoteBuildRequest.Builder().repositoryType("S3")
            .repositoryName("MyVectorStore")
            .objectPath("MyObjectPath")
            .tenantId("MyTenant")
            .numDocs("100000")
            .vectorDataType("float")
            .addIndexParameter("dimension", 128)
            .addIndexParameter("space_type", "l2")
            .setMethod("hnsw", methodParams)
            .build();

        String requestBody = new ObjectMapper().writeValueAsString(remoteBuildRequest.toJson());

        SdkHttpFullRequest request = SdkHttpFullRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(endpoint)
            .appendHeader("Content-Type", "application/json")
            .build();

        return HttpExecuteRequest.builder().contentStreamProvider(() -> new StringInputStream(requestBody)).request(request).build();
    }

    public HttpExecuteResponse getBuildStatus(String jobId) throws IOException, URISyntaxException {
        // URI endpoint = new URL(KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_ENDPOINT) + "/_status").toURI();
        URI endpoint = new URL("http://localhost:8888" + "/_status").toURI();
        SdkHttpRequest request = SdkHttpRequest.builder().method(SdkHttpMethod.POST).uri(endpoint).build();
        return httpClient.prepareRequest(HttpExecuteRequest.builder().request(request).build()).call();
    }

    public void cancelBuild(String jobId) {
        String endpoint = KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_ENDPOINT) + "/_cancel/" + jobId;
    }

    public void rebuildClient() {
        if (httpClient != null) {
            httpClient.close();
        }
        httpClient = createHttpClient();
    }

    public void close() {
        if (httpClient != null) {
            httpClient.close();
        }
    }
}
