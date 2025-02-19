/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.remote;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.log4j.Log4j2;
import org.apache.hc.client5.http.ContextBuilder;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.BasicHttpClientResponseHandler;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.DefaultBackoffStrategy;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.settings.SecureString;
import org.opensearch.knn.index.KNNSettings;

import java.io.IOException;
import java.net.URISyntaxException;
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
    private volatile CloseableHttpClient httpClient;

    public static final int MAX_RETRIES = 1; // 2 total attempts
    public static final long BASE_DELAY_MS = 100;

    private HttpClientContext clientContext;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    RemoteIndexClient() {
        this.httpClient = createHttpClient();
        SecureString username = KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_USERNAME);
        SecureString password = KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_PASSWORD);
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
    private CloseableHttpClient createHttpClient() {
        // HttpHost host = new HttpHost(String.valueOf(KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_ENDPOINT)));

        HttpHost host = new HttpHost("http://localhost:8888");

        // TODO fetch creds from Keystore

        this.clientContext = ContextBuilder.create()
            .preemptiveBasicAuth(host, new UsernamePasswordCredentials("demo", "demo".toCharArray()))
            .build();

        return HttpClients.custom()
            .setRetryStrategy(new RemoteIndexClientRetryStrategy())
            .setConnectionBackoffStrategy(new DefaultBackoffStrategy())
            .build();
    }

    /**
    * Submit a build to the Remote Vector Build Service endpoint
    * @return job_id from the server response used to track the job
    */
    public String submitVectorBuild() throws IOException {
        HttpPost buildRequest = constructBuildRequest();
        String response = httpClient.execute(buildRequest, clientContext, new BasicHttpClientResponseHandler());
        return getValueFromResponse(response, "job_id");
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
            String response = getBuildStatus(jobId);
            String status = getValueFromResponse(response, "task_status");

            // TODO Return full response for error message in FAILED and build path in COMPLETED

            switch (status) {
                case "COMPLETED_INDEX_BUILD":
                    if (getValueFromResponse(response, "index_path") == null) {
                        throw new InterruptedException("Index build reported completed without an index path.");
                    }
                    return getValueFromResponse(response, "index_path"); // return path from response
                case "FAILED_INDEX_BUILD":
                    String errorMsg = getValueFromResponse(response, "error");
                    if (errorMsg != null) {
                        throw new InterruptedException("Index build failed: " + errorMsg);
                    }
                    throw new InterruptedException("Index build failed without an error message.");
                case "RUNNING_INDEX_BUILD":
                    Thread.sleep(pollInterval);
            }
        }
        // try {
        // cancelBuild(jobId);
        // } catch (Exception e) {
        // log.error("Failed to cancel build after timeout", e);
        // }
        throw new InterruptedException("Build timed out, falling back to CPU build.");
    }

    /**
     * Helper method to directly get the status response for a given job ID
     * @param jobId to check
     * @return HttpExecuteResponse for the status request
     */
    public String getBuildStatus(String jobId) throws IOException {
        // URI endpoint = new URL(KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_ENDPOINT) + "/_status").toURI();
        HttpGet request = new HttpGet("http://localhost:8888/_status/" + jobId);
        return httpClient.execute(request, clientContext, new BasicHttpClientResponseHandler());
    }

    /**
     * Construct the JSON request body and HTTP request for the index build request
     * @return HttpExecuteRequest for the index build request with parameters set
     */
    public HttpPost constructBuildRequest() throws IOException {
        // URI endpoint = new URL(KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_ENDPOINT) + "/_build").toURI();

        HttpPost request = new HttpPost("http://localhost:8888/_build");
        request.setHeader("Content-Type", "application/json");

        Map<String, Object> methodParams = new HashMap<>();
        methodParams.put("ef_construction", 128);
        methodParams.put("m", 16);

        RemoteBuildRequest remoteBuildRequest = RemoteBuildRequest.builder()
            .repositoryType("S3")
            .containerName("MyVectorStore")
            .objectPath("MyObjectPath")
            .tenantId("MyTenant")
            .dimension(768)
            .docCount(1_000_000)
            .dataType("fp32")
            .engine("faiss")
            .setAlgorithm("hnsw")
            .setAlgorithmParameters(methodParams)
            .addIndexParameter("space_type", "l2")
            .build();

        request.setEntity(new StringEntity(remoteBuildRequest.toJson()));

        return request;
    }

    /**
    * Given a JSON response string, get a value for a specific key. Converts json {@literal <null>} to Java null.
    * @param responseBody The response to read
    * @param key The key to lookup
    * @return The value for the key, or null if not found
    */
    public static String getValueFromResponse(String responseBody, String key) throws JsonProcessingException {
        ObjectNode jsonResponse = (ObjectNode) objectMapper.readTree(responseBody);
        if (jsonResponse.has(key)) {
            if (jsonResponse.get(key).isNull()) {
                return null;
            }
            return jsonResponse.get(key).asText();
        }
        throw new IllegalArgumentException("Key " + key + " not found in response");
    }

    /**
     * Rebuild the httpClient
     */
    public void rebuildClient() throws IOException {
        close();
        httpClient = createHttpClient();
    }

    /**
     * Close the httpClient
     */
    public void close() throws IOException {
        if (httpClient != null) {
            httpClient.close();
        }
    }
}
