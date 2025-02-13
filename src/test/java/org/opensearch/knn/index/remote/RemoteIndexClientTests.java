/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.remote;

import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;

public class RemoteIndexClientTests {
    // @Mock
    // protected ClusterService clusterService;
    // @Mock
    // protected ClusterSettings clusterSettings;

    @After
    public void tearDown() throws Exception {
        RemoteIndexClient.getInstance().close();
    }

    @Test
    public void testBuildRequest() throws IOException, URISyntaxException {
        RemoteIndexClient remoteIndexClient = RemoteIndexClient.getInstance();
        assertEquals(123456, Integer.parseInt(remoteIndexClient.submitVectorBuild()));
    }
}
