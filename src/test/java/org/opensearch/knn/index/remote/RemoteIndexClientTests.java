/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.remote;

import org.junit.After;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.knn.KNNTestCase;
import org.opensearch.knn.index.KNNSettings;

import java.io.IOException;
import java.net.URISyntaxException;

public class RemoteIndexClientTests extends KNNTestCase {
    @Mock
    protected ClusterService clusterService;
    @Mock
    protected ClusterSettings clusterSettings;

    protected AutoCloseable openMocks;

    private static MockedStatic<KNNSettings> knnSettingsMockedStatic;

    @After
    public void tearDown() throws Exception {
        RemoteIndexClient.getInstance().close();
        super.tearDown();
    }

    public void testBuildRequest() throws IOException, URISyntaxException {
        RemoteIndexClient remoteIndexClient = RemoteIndexClient.getInstance();
        remoteIndexClient.submitVectorBuild();
    }
}
