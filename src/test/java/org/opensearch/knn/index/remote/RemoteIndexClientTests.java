/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.remote;

import com.google.common.collect.ImmutableSet;
import lombok.SneakyThrows;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.knn.index.KNNSettings;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.knn.index.KNNSettings.KNN_REMOTE_BUILD_SERVICE_ENDPOINT;
import static org.opensearch.knn.index.KNNSettings.QUANTIZATION_STATE_CACHE_EXPIRY_TIME_MINUTES_SETTING;
import static org.opensearch.knn.index.KNNSettings.QUANTIZATION_STATE_CACHE_SIZE_LIMIT_SETTING;

public class RemoteIndexClientTests extends OpenSearchSingleNodeTestCase {
    @Mock
    protected ClusterService clusterService;
    @Mock
    protected ClusterSettings clusterSettings;

    protected AutoCloseable openMocks;

    @Before
    public void setup() {
        openMocks = MockitoAnnotations.openMocks(this);
        clusterService = mock(ClusterService.class);
        Set<Setting<?>> defaultClusterSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        defaultClusterSettings.addAll(
            KNNSettings.state()
                .getSettings()
                .stream()
                .filter(s -> s.getProperties().contains(Setting.Property.NodeScope))
                .collect(Collectors.toList())
        );
        KNNSettings.state().setClusterService(clusterService);
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(Settings.EMPTY, defaultClusterSettings));
        when(KNNSettings.state().getSettingValue(KNN_REMOTE_BUILD_SERVICE_ENDPOINT)).thenReturn("localhost");
    }

    @Override
    public void tearDown() throws Exception {
        // Clear out persistent metadata
        ClusterUpdateSettingsRequest clusterUpdateSettingsRequest = new ClusterUpdateSettingsRequest();
        client().admin().cluster().updateSettings(clusterUpdateSettingsRequest).get();
        super.tearDown();
    }

    public void testGet() throws IOException {
        RemoteIndexClient remoteIndexClient = RemoteIndexClient.getInstance();
        remoteIndexClient.submitVectorBuild();
    }

    @SneakyThrows
    public void testRebuildOnCacheSizeSettingsChange() {
        Settings settings = Settings.builder().build();
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            ImmutableSet.of(QUANTIZATION_STATE_CACHE_SIZE_LIMIT_SETTING, QUANTIZATION_STATE_CACHE_EXPIRY_TIME_MINUTES_SETTING)
        );
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.getSettings()).thenReturn(settings);

        Client client = mock(Client.class);

        KNNSettings.state().initialize(client, clusterService);

        RemoteIndexClient remoteIndexClient = RemoteIndexClient.getInstance();

        // Settings newSettings = Settings.builder().put(KNN_REMOTE_BUILD_SERVICE_ENDPOINT_SETTING, ).build();
    }

}
