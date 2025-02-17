/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.remote;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import java.util.concurrent.TimeUnit;

/**
 * Class to handle all interactions with the remote vector build service.
 * InterruptedExceptions will cause a fallback to local CPU build.
 */
@Log4j2
public class RemoteIndexClient {

    private static RemoteIndexClient INSTANCE;
    private volatile SdkHttpClient httpClient;

    RemoteIndexClient() {
        this.httpClient = createHttpClient();
    }

    /**
     * Return the Singleton instance of the node's RemoteIndexClient
     * @return RemoteIndexClient instance
     */
    public static synchronized RemoteIndexClient getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new RemoteIndexClient();
        }
        return INSTANCE;
    }

    /**
     * Initialize the httpClient to be used
     * @return The HTTP Client
     */
    private SdkHttpClient createHttpClient() {
        // HttpHost host = new HttpHost(String.valueOf(KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_ENDPOINT)));
        HttpHost host = new HttpHost("localhost", 8888, "http");
        AuthScope authScope = new AuthScope(host);
        BasicCredentialsProvider basicCredentialsProvider = new BasicCredentialsProvider();
        // TODO fetch creds from Keystore
        basicCredentialsProvider.setCredentials(authScope, new UsernamePasswordCredentials("demo", "demo"));

        return ApacheHttpClient.builder().build();
    }

    /**
     * Submit a build to the Remote Vector Build Service endpoint
     * @return job_id from the server response used to track the job
     */
    public String submitVectorBuild() throws IOException, URISyntaxException {
        HttpExecuteRequest buildRequest = constructBuildRequest();
        HttpExecuteResponse response = httpClient.prepareRequest(buildRequest).call();

        if (response.httpResponse().statusCode() != 200) {
            throw new RuntimeException("Failed to submit vector build");
        }

        return getValueFromResponse(getResponseString(response), "job_id");
    }

    /**
     * Await the completion of the index build by polling periodically and handling the returned statuses.
     * @param jobId identifier from the server to track the job
     * @return the path to the completed index
     * @throws InterruptedException on a timeout or terminal failure from the server to trigger local CPU fallback
     */
    @SuppressWarnings("BusyWait")
    public String awaitVectorBuild(String jobId) throws InterruptedException, IOException, URISyntaxException {
        long startTime = System.currentTimeMillis();
        // long timeout = ((TimeValue) (KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_TIMEOUT))).getMillis();
        long timeout = new TimeValue(10, TimeUnit.MINUTES).getMillis();
        // long pollInterval = KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_POLL_INTERVAL);
        long pollInterval = new TimeValue(30, TimeUnit.SECONDS).getMillis();

        // Thread.sleep(1000 * 60); // TODO Set initial delay

        while (System.currentTimeMillis() - startTime < timeout) {
            HttpExecuteResponse statusResponse = getBuildStatus(jobId);
            String responseBody = getResponseString(statusResponse);
            String status = getValueFromResponse(responseBody, "task_status");
            String buildPath = getValueFromResponse(responseBody, "index_path");

            // TODO Return full response for error message in FAILED and build path in COMPLETED

            switch (status) {
                case "COMPLETED_INDEX_BUILD":
                    return buildPath; // return path from response
                case "FAILED_INDEX_BUILD":
                    throw new InterruptedException("Index build failed");
                // TODO getValue("status") in case we decide we want more context on failure
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
        throw new InterruptedException("Build timed out, falling back to CPU build.");
    }

    /**
     * Helper method to directly get the status response for a given job ID
     * @param jobId to check
     * @return HttpExecuteResponse for the status request
     */
    public HttpExecuteResponse getBuildStatus(String jobId) throws IOException, URISyntaxException {
        // URI endpoint = new URL(KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_ENDPOINT) + "/_status").toURI();
        URI endpoint = new URL("http://localhost:8888" + "/_status/" + jobId).toURI();
        SdkHttpRequest request = SdkHttpRequest.builder().method(SdkHttpMethod.GET).uri(endpoint).build();
        return httpClient.prepareRequest(HttpExecuteRequest.builder().request(request).build()).call();
    }

    /**
     * Cancel a build by job ID
     * @param jobId to cancel
     */
    public void cancelBuild(String jobId) {
        String endpoint = KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_ENDPOINT) + "/_cancel/" + jobId;
        SdkHttpRequest request = SdkHttpRequest.builder().method(SdkHttpMethod.POST).uri(URI.create(endpoint)).build();
        try {
            httpClient.prepareRequest(HttpExecuteRequest.builder().request(request).build()).call();
        } catch (IOException e) {
            throw new RuntimeException("Failed to cancel build", e);
        }
    }

    /**
     * Construct the JSON request body and HTTP request for the index build request
     * @return HttpExecuteRequest for the index build request with parameters set
     */
    public HttpExecuteRequest constructBuildRequest() throws MalformedURLException, URISyntaxException, JsonProcessingException {
        // URI endpoint = new URL(KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_ENDPOINT) + "/_build").toURI();
        URI endpoint = new URL("http://localhost:8888" + "/_build").toURI();

        Map<String, Object> methodParams = new HashMap<>();
        methodParams.put("ef_construction", 128);
        methodParams.put("m", 16);

        RemoteBuildRequest remoteBuildRequest = RemoteBuildRequest.builder()
            .repositoryType("S3")
            .repositoryName("MyVectorStore")
            .objectPath("MyObjectPath")
            .tenantId("MyTenant")
            .docCount(100000)
            .dataType("float")
            .dimension(128)
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

    /**
     * Get an HTTP response body as a string. Future invocations of readAllBytes will return an empty byte array.
     * @param response HTTP response to read from
     * @return String of the response body
     */
    public String getResponseString(HttpExecuteResponse response) {
        if (response.httpResponse().isSuccessful()) {
            return response.responseBody().map(body -> {
                try {
                    return new String(body.readAllBytes(), StandardCharsets.UTF_8);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to read response body", e);
                }
            }).orElseThrow(() -> new RuntimeException("Response body is empty"));
        }
        return null;
    }

    /**
     * Given a response string, get a value for a specific JSON String key.
     * @param responseBody The response to read
     * @param key The key to lookup
     * @return The value for the key, or null if not found
     */
    public String getValueFromResponse(String responseBody, String key) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode jsonResponse = (ObjectNode) mapper.readTree(responseBody);
        if (jsonResponse.has(key)) {
            return jsonResponse.get(key).asText();
        }
        return null;
    }

    /**
     * Rebuild the httpClient
     */
    public void rebuildClient() {
        if (httpClient != null) {
            httpClient.close();
        }
        httpClient = createHttpClient();
    }

    /**
     * Close the httpClient
     */
    public void close() {
        if (httpClient != null) {
            httpClient.close();
        }
    }
}
