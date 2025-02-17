/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.remote;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.junit.After;
import org.junit.Test;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class RemoteIndexClientTests {

    @After
    public void tearDown() {
        RemoteIndexClient.getInstance().close();
    }

    @Test
    public void testGetSdkHttpClient_success() {
        SdkHttpClient client = ApacheHttpClient.builder().build();
        assertNotNull(client);
    }

    @Test
    public void testConstructBuildRequest() throws JsonProcessingException {
        Map<String, Object> methodParams = new HashMap<>();
        methodParams.put("ef_construction", 128);
        methodParams.put("m", 16);

        RemoteBuildRequest request = RemoteBuildRequest.builder()
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

        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(request.toJson());

        System.out.println(json);
    }

    @Test
    @SneakyThrows
    public void testBuildRequest() {
        RemoteIndexClient remoteIndexClient = RemoteIndexClient.getInstance();
        assertEquals(123456, Integer.parseInt(remoteIndexClient.submitVectorBuild()));
    }

    @Test
    @SneakyThrows
    public void testPolling() {
        RemoteIndexClient remoteIndexClient = RemoteIndexClient.getInstance();
        remoteIndexClient.awaitVectorBuild(remoteIndexClient.submitVectorBuild());
    }

    @Test
    public void testCancel() {
        RemoteIndexClient remoteIndexClient = RemoteIndexClient.getInstance();
    }
}
