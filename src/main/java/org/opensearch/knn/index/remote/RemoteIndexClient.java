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
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

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
        HttpHost host = new HttpHost(String.valueOf(KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_ENDPOINT)));

        SecureString username = KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_USERNAME);
        SecureString password = KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_PASSWORD);

        if (password == null) {
            this.clientContext = null;
        } else {
            try (SecureString copy = password.clone()) {
                this.clientContext = ContextBuilder.create()
                    .preemptiveBasicAuth(host, new UsernamePasswordCredentials(username.toString(), copy.getChars()))
                    .build();
            }
        }

        return HttpClients.custom()
            .setRetryStrategy(new RemoteIndexClientRetryStrategy())
            .setConnectionBackoffStrategy(new DefaultBackoffStrategy())
            .build();
    }

    /**
    * Submit a build to the Remote Vector Build Service endpoint
    * @return job_id from the server response used to track the job
    */
    public String submitVectorBuild() throws IOException, URISyntaxException {
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
        long timeout = ((TimeValue) (KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_TIMEOUT))).getMillis();
        long pollInterval = KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_POLL_INTERVAL);

        Thread.sleep(1000 * 60);

        while (System.currentTimeMillis() - startTime < timeout) {
            String response = getBuildStatus(jobId);
            String status = getValueFromResponse(response, "task_status");
            if (status == null) {
                throw new InterruptedException("Build status response did not contain a status.");
            }

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
        throw new InterruptedException("Build timed out, falling back to CPU build.");
    }

    /**
     * Helper method to directly get the status response for a given job ID
     * @param jobId to check
     * @return HttpExecuteResponse for the status request
     */
    public String getBuildStatus(String jobId) throws IOException, URISyntaxException {
        URI endpoint = new URL(KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_ENDPOINT) + "/_status/").toURI();
        HttpGet request = new HttpGet(endpoint + jobId);
        return httpClient.execute(request, clientContext, new BasicHttpClientResponseHandler());
    }

    /**
     * Construct the JSON request body and HTTP request for the index build request
     * @return HttpExecuteRequest for the index build request with parameters set
     */
    public HttpPost constructBuildRequest() throws IOException, URISyntaxException {
        URI endpoint = new URL(KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_ENDPOINT) + "/_build").toURI();

        HttpPost request = new HttpPost(endpoint);
        request.setHeader("Content-Type", "application/json");

        Map<String, Object> algorithmParams = new HashMap<>();
        algorithmParams.put("ef_construction", 100);
        algorithmParams.put("m", 16);

        Map<String, Object> indexParameters = new HashMap<>();
        indexParameters.put("algorithm", "hnsw");
        indexParameters.put("algorithm_parameters", algorithmParams);

        RemoteBuildRequest remoteBuildRequest = RemoteBuildRequest.builder()
            .repositoryType("S3")
            .containerName("MyVectorStore")
            .objectPath("MyObjectPath")
            .tenantId("MyTenant")
            .dimension(768)
            .docCount(1_000_000)
            .dataType("fp32")
            .engine("faiss")
            .indexParameters(indexParameters)
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
