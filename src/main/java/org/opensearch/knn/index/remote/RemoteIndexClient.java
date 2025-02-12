/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.remote;

import lombok.extern.log4j.Log4j2;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.knn.index.KNNSettings;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.apache.ApacheHttpClient;

import java.io.IOException;

@Log4j2
public class RemoteIndexClient {
    private static RemoteIndexClient INSTANCE;

    private volatile SdkHttpClient httpClient;

    private RemoteIndexClient() {
        initialize();
    }

    private void initialize() {
        this.httpClient = createHttpClient();
    }

    public static synchronized RemoteIndexClient getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new RemoteIndexClient();
        }
        return INSTANCE;
    }

    private SdkHttpClient createHttpClient() {
        return ApacheHttpClient.create();

        // TODO add credentials from OpenSearch keystore

    }

    public String submitVectorBuild() throws IOException {
        SdkHttpFullRequest buildRequest = constructBuildRequest();
        ExecutableHttpRequest executableHttpRequest = httpClient.prepareRequest(null);
        HttpExecuteResponse response = executableHttpRequest.call();
        String jobID = null;
        if (response.httpResponse().isSuccessful()) {
            jobID = response.responseBody().toString();
        }
        return jobID;
    }

    @SuppressWarnings("BusyWait")
    public String awaitVectorBuild(String jobId) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        long timeout = ((TimeValue) (KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_TIMEOUT))).getMinutes();
        long pollInterval = KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_POLL_INTERVAL);

        while (System.currentTimeMillis() - startTime < timeout) {
            String status = getBuildStatus(jobId);

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

    public SdkHttpFullRequest constructBuildRequest() {
        String endpoint = KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_ENDPOINT) + "/_build";
        SdkHttpFullRequest.builder().encodedPath(endpoint);
        return null;
    }

    public String getBuildStatus(String jobId) {
        String endpoint = KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_ENDPOINT) + "/_status/" + jobId;
        return null;
    }

    public void cancelBuild(String jobId) {
        String endpoint = KNNSettings.state().getSettingValue(KNNSettings.KNN_REMOTE_BUILD_SERVICE_ENDPOINT) + "/_cancel/" + jobId;
    }

    public void rebuildClient() {
        if (httpClient != null) {
            httpClient.close();
        }
        initialize();
    }
}
