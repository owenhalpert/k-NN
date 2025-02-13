/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.remote;

import lombok.SneakyThrows;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RemoteIndexClientTests {

    @After
    public void tearDown() {
        RemoteIndexClient.getInstance().close();
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
}
