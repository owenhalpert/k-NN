/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.remote;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.log4j.Log4j2;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.impl.classic.BasicHttpClientResponseHandler;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.DefaultBackoffStrategy;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.utils.Base64;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.settings.SecureString;
import org.opensearch.knn.index.KNNSettings;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class to handle all interactions with the remote vector build service.
 * InterruptedExceptions will cause a fallback to local CPU build.
 */
@Log4j2
public class RemoteIndexClient {
    private static RemoteIndexClient INSTANCE;
    private volatile CloseableHttpClient httpClient;

    public static final int MAX_RETRIES = 1; // 2 total attempts
    public static final long BASE_DELAY_MS = 1000;
    public static final long INITIAL_DELAY_MS = 1000;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    // To be used for round-robin task assignment to know which endpoint accepted the given job.
    private final Map<String, String> jobEndpoints = new ConcurrentHashMap<>();

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
        return HttpClients.custom()
            .setRetryStrategy(new RemoteIndexClientRetryStrategy())
            .setConnectionBackoffStrategy(new DefaultBackoffStrategy())
            .build();
    }

    /**
    * Submit a build to the Remote Vector Build Service endpoint using round robin task assignment.
    * @return job_id from the server response used to track the job
    */
    public String submitVectorBuild() throws IOException, InterruptedException {
        List<String> endpoints = KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_ENDPOINT);

        if (endpoints.isEmpty()) {
            throw new InterruptedException("No endpoints found");
        }

        int i = 0;
        while (i < endpoints.size()) {
            HttpPost buildRequest = constructBuildRequest(URI.create(endpoints.get(i)));
            authenticateRequest(buildRequest);
            String response = httpClient.execute(buildRequest, body -> {
                if (body.getCode() == 507) {
                    return null;
                }
                if (body.getCode() != 200) {
                    throw new IOException("Failed to submit build request after retries with code: " + body.getCode());
                }
                return EntityUtils.toString(body.getEntity());
            });
            if (response != null) {
                String jobId = getValueFromResponse(response, "job_id");
                jobEndpoints.put(jobId, endpoints.get(i));
                return jobId;
            }
            i++;
        }
        throw new InterruptedException("All build service endpoints rejected the build request. Falling back to CPU build.");
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
        long pollInterval = ((TimeValue) (KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_POLL_INTERVAL)))
            .getMillis();

        Thread.sleep(INITIAL_DELAY_MS);

        while (System.currentTimeMillis() - startTime < timeout) {
            String response = getBuildStatus(jobId);
            String status = getValueFromResponse(response, "task_status");
            if (status == null) {
                throw new InterruptedException("Build status response did not contain a status.");
            }

            switch (status) {
                case "COMPLETED_INDEX_BUILD":
                    String indexPath = getValueFromResponse(response, "index_path");
                    if (indexPath == null) {
                        throw new InterruptedException("Index build reported completed without an index path.");
                    }
                    jobEndpoints.remove(jobId);
                    return indexPath;
                case "FAILED_INDEX_BUILD":
                    String errorMsg = getValueFromResponse(response, "error");
                    if (errorMsg != null) {
                        throw new InterruptedException("Index build failed: " + errorMsg);
                    }
                    jobEndpoints.remove(jobId);
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
        String endpointString = jobEndpoints.get(jobId);
        if (endpointString == null) {
            throw new IOException("Job ID not found in jobEndpoints map");
        }
        URI endpoint = new URI(endpointString);
        HttpGet request = new HttpGet(endpoint + "/_status/" + jobId);
        authenticateRequest(request);
        return httpClient.execute(request, new BasicHttpClientResponseHandler());
    }

    /**
     * Construct the JSON request body and HTTP request for the index build request
     * @return HttpExecuteRequest for the index build request with parameters set
     */
    public HttpPost constructBuildRequest(URI endpoint) throws IOException {
        // TODO Fetch parameters from repository settings and k-NN index parameters
        HttpPost request = new HttpPost(endpoint + "/_build");
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
        // TODO See if I can use OpenSearch XContent tools here to avoid Jackson dependency
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
     * Authenticate the HTTP request by manually setting the auth header. This is done manually per request because
     * the endpoints are not known ahead of time to set a global auth scheme. Allows for dynamic credential updates.
     * @param request to be authenticated
     */
    public void authenticateRequest(HttpUriRequestBase request) {
        SecureString username = KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_USERNAME);
        SecureString password = KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_PASSWORD);

        if (password != null) {
            final String auth = username + ":" + password.clone();
            final byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.ISO_8859_1));
            final String authHeader = "Basic " + new String(encodedAuth);
            request.setHeader(HttpHeaders.AUTHORIZATION, authHeader);
        }
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
