/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.nativeindex.remote;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.Version;
import org.junit.Before;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.SetOnce;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.blobstore.stream.read.ReadContext;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexSettings;
import org.opensearch.knn.KNNTestCase;
import org.opensearch.knn.index.KNNSettings;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.knn.KNNTestCase;
import org.opensearch.knn.common.KNNConstants;
import org.opensearch.knn.index.KNNSettings;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.codec.nativeindex.NativeIndexBuildStrategy;
import org.opensearch.knn.index.codec.nativeindex.model.BuildIndexParams;
import org.opensearch.knn.index.engine.KNNEngine;
import org.opensearch.knn.index.remote.RemoteBuildRequest;
import org.opensearch.knn.index.store.IndexOutputWithBuffer;
import org.opensearch.knn.index.vectorvalues.KNNVectorValues;
import org.opensearch.knn.index.vectorvalues.KNNVectorValuesFactory;
import org.opensearch.knn.index.vectorvalues.TestVectorValues;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.knn.index.KNNSettings.KNN_REMOTE_VECTOR_REPO_SETTING;

public class RemoteIndexBuildStrategyTests extends KNNTestCase {

    private record TestIndexBuildStrategy(SetOnce<Boolean> fallback) implements NativeIndexBuildStrategy {
        @Override
        public void buildAndWriteIndex(BuildIndexParams indexInfo) throws IOException {
            fallback.set(true);
        }
    }

    private static class TestBlobContainer extends FsBlobContainer implements AsyncMultiStreamBlobContainer {

        private final SetOnce<Boolean> asyncUpload;
        private final SetOnce<Boolean> upload;
        private final boolean throwsException;

        public TestBlobContainer(
            FsBlobStore blobStore,
            BlobPath blobPath,
            Path path,
            SetOnce<Boolean> asyncUpload,
            SetOnce<Boolean> upload,
            boolean throwsException
        ) {
            super(blobStore, blobPath, path);
            this.asyncUpload = asyncUpload;
            this.upload = upload;
            this.throwsException = throwsException;

        }

        @Override
        public void asyncBlobUpload(WriteContext writeContext, ActionListener<Void> actionListener) throws IOException {
            asyncUpload.set(true);
            if (this.throwsException) {
                actionListener.onFailure(new IOException("Test Exception"));
            } else {
                actionListener.onResponse(null);
            }
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            upload.set(true);
        }

        @Override
        public void readBlobAsync(String s, ActionListener<ReadContext> actionListener) {}

        @Override
        public boolean remoteIntegrityCheckSupported() {
            return false;
        }

        @Override
        public void deleteAsync(ActionListener<DeleteResult> actionListener) {}

        @Override
        public void deleteBlobsAsyncIgnoringIfNotExists(List<String> list, ActionListener<Void> actionListener) {}
    }

    final List<float[]> vectorValues = List.of(new float[] { 1, 2 }, new float[] { 2, 3 }, new float[] { 3, 4 });
    final TestVectorValues.PreDefinedFloatVectorValues randomVectorValues = new TestVectorValues.PreDefinedFloatVectorValues(vectorValues);
    final Supplier<KNNVectorValues<?>> knnVectorValuesSupplier = KNNVectorValuesFactory.getVectorValuesSupplier(
        VectorDataType.FLOAT,
        randomVectorValues
    );
    final IndexOutputWithBuffer indexOutputWithBuffer = Mockito.mock(IndexOutputWithBuffer.class);
    final String segmentName = "test-segment-name";
    final SegmentInfo segmentInfo = new SegmentInfo(
        mock(Directory.class),
        mock(Version.class),
        mock(Version.class),
        segmentName,
        0,
        false,
        false,
        mock(Codec.class),
        mock(Map.class),
        new byte[16],
        mock(Map.class),
        mock(Sort.class)
    );
    final SegmentWriteState segmentWriteState = new SegmentWriteState(
        mock(InfoStream.class),
        mock(Directory.class),
        segmentInfo,
        mock(FieldInfos.class),
        null,
        mock(IOContext.class)
    );
    final KNNVectorValues<?> knnVectorValues = knnVectorValuesSupplier.get();
    final BuildIndexParams buildIndexParams = BuildIndexParams.builder()
        .indexOutputWithBuffer(indexOutputWithBuffer)
        .knnEngine(KNNEngine.FAISS)
        .vectorDataType(VectorDataType.FLOAT)
        .parameters(Map.of("index", "param"))
        .knnVectorValuesSupplier(knnVectorValuesSupplier)
        .totalLiveDocs((int) knnVectorValues.totalLiveDocs())
        .segmentWriteState(segmentWriteState)
        .build();

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        ClusterSettings clusterSettings = mock(ClusterSettings.class);
        when(clusterSettings.get(KNN_REMOTE_VECTOR_REPO_SETTING)).thenReturn("test-repo-name");
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        KNNSettings.state().setClusterService(clusterService);
    }

    /**
     * Test that we fallback to the fallback NativeIndexBuildStrategy when an exception is thrown
     */
    public void testFallback() throws IOException {
        RepositoriesService repositoriesService = mock(RepositoriesService.class);
        when(repositoriesService.repository(any())).thenThrow(new RepositoryMissingException("Fallback"));

        final SetOnce<Boolean> fallback = new SetOnce<>();
        RemoteIndexBuildStrategy objectUnderTest = new RemoteIndexBuildStrategy(
            () -> repositoriesService,
            new TestIndexBuildStrategy(fallback),
            mock(IndexSettings.class)
        );
        objectUnderTest.buildAndWriteIndex(buildIndexParams);
        assertTrue(fallback.get());
    }

    /**
     * Test that whenever an AsyncMultiStreamBlobContainer is used, both asyncBlobUpload and writeBlob are invoked once and only once
     */
    public void testRepositoryInteraction() throws IOException, InterruptedException {
        RepositoriesService repositoriesService = mock(RepositoriesService.class);
        BlobStoreRepository mockRepository = mock(BlobStoreRepository.class);
        BlobPath testBasePath = new BlobPath().add("testBasePath");
        BlobStore mockBlobStore = mock(BlobStore.class);

        when(repositoriesService.repository(any())).thenReturn(mockRepository);
        when(mockRepository.basePath()).thenReturn(testBasePath);
        when(mockRepository.blobStore()).thenReturn(mockBlobStore);

        final SetOnce<Boolean> asyncUpload = new SetOnce<>();
        final SetOnce<Boolean> upload = new SetOnce<>();
        AsyncMultiStreamBlobContainer testContainer = new TestBlobContainer(
            mock(FsBlobStore.class),
            testBasePath,
            mock(Path.class),
            asyncUpload,
            upload,
            false
        );
        when(mockBlobStore.blobContainer(any())).thenReturn(testContainer);

        RemoteIndexBuildStrategy objectUnderTest = new RemoteIndexBuildStrategy(
            () -> repositoriesService,
            mock(NativeIndexBuildStrategy.class),
            mock(IndexSettings.class)
        );

        objectUnderTest.writeToRepository("test_blob", 100, VectorDataType.FLOAT, knnVectorValuesSupplier);

        assertTrue(asyncUpload.get());
        assertTrue(upload.get());
        verify(mockBlobStore).blobContainer(any());
        verify(mockRepository).basePath();
    }

    /**
     * Test that when an exception is thrown during asyncBlobUpload, the exception is rethrown.
     */
    public void testAsyncUploadThrowsException() throws InterruptedException, IOException {
        RepositoriesService repositoriesService = mock(RepositoriesService.class);
        BlobStoreRepository mockRepository = mock(BlobStoreRepository.class);
        BlobPath testBasePath = new BlobPath().add("testBasePath");
        BlobStore mockBlobStore = mock(BlobStore.class);

        when(repositoriesService.repository(any())).thenReturn(mockRepository);
        when(mockRepository.basePath()).thenReturn(testBasePath);
        when(mockRepository.blobStore()).thenReturn(mockBlobStore);

        final SetOnce<Boolean> asyncUpload = new SetOnce<>();
        final SetOnce<Boolean> upload = new SetOnce<>();
        AsyncMultiStreamBlobContainer testContainer = new TestBlobContainer(
            mock(FsBlobStore.class),
            testBasePath,
            mock(Path.class),
            asyncUpload,
            upload,
            true
        );
        when(mockBlobStore.blobContainer(any())).thenReturn(testContainer);

        final SetOnce<Boolean> fallback = new SetOnce<>();
        RemoteIndexBuildStrategy objectUnderTest = new RemoteIndexBuildStrategy(
            () -> repositoriesService,
            new TestIndexBuildStrategy(fallback),
            mock(IndexSettings.class)
        );

        assertThrows(
            IOException.class,
            () -> objectUnderTest.writeToRepository("test_blob", 100, VectorDataType.FLOAT, knnVectorValuesSupplier)
        );

        assertTrue(asyncUpload.get());
        assertTrue(upload.get());
        verify(mockBlobStore).blobContainer(any());
        verify(mockRepository).basePath();

    }

    public void testBuildRequest() throws IOException {
        RepositoriesService repositoriesService = mock(RepositoriesService.class);
        BlobStoreRepository blobStoreRepository = mock(BlobStoreRepository.class);
        RepositoryMetadata metadata = mock(RepositoryMetadata.class);
        Settings repoSettings = Settings.builder().put("bucket", "test-bucket").build();

        when(metadata.type()).thenReturn("s3");
        when(metadata.settings()).thenReturn(repoSettings);
        when(blobStoreRepository.getMetadata()).thenReturn(metadata);
        when(repositoriesService.repository("test-repo")).thenReturn(blobStoreRepository);

        KNNSettings knnSettingsMock = mock(KNNSettings.class);
        when(knnSettingsMock.getSettingValue(KNN_REMOTE_VECTOR_REPO_SETTING.getKey())).thenReturn("test-repo");

        IndexSettings mockIndexSettings = mock(IndexSettings.class);
        Settings indexSettingsSettings = Settings.builder().put(ClusterName.CLUSTER_NAME_SETTING.getKey(), "test-cluster").build();
        when(mockIndexSettings.getSettings()).thenReturn(indexSettingsSettings);

        try (MockedStatic<KNNSettings> knnSettingsStaticMock = Mockito.mockStatic(KNNSettings.class)) {
            knnSettingsStaticMock.when(KNNSettings::state).thenReturn(knnSettingsMock);

            final SetOnce<Boolean> fallback = new SetOnce<>();
            RemoteIndexBuildStrategy objectUnderTest = new RemoteIndexBuildStrategy(
                () -> repositoriesService,
                new TestIndexBuildStrategy(fallback),
                mockIndexSettings
            );

            List<float[]> vectorValues = List.of(new float[] { 1, 2 }, new float[] { 2, 3 });
            final TestVectorValues.PreDefinedFloatVectorValues randomVectorValues = new TestVectorValues.PreDefinedFloatVectorValues(
                vectorValues
            );
            final KNNVectorValues<byte[]> knnVectorValues = KNNVectorValuesFactory.getVectorValues(
                VectorDataType.FLOAT,
                randomVectorValues
            );

            BuildIndexParams buildIndexParams = BuildIndexParams.builder()
                .knnEngine(KNNEngine.FAISS)
                .vectorDataType(VectorDataType.FLOAT)
                .parameters(Map.of(KNNConstants.SPACE_TYPE, "l2"))
                .knnVectorValuesSupplier(() -> knnVectorValues)
                .totalLiveDocs(vectorValues.size())
                .build();

            RemoteBuildRequest request = objectUnderTest.constructBuildRequest(buildIndexParams, "blob");

            assertEquals("s3", request.getRepositoryType());
            assertEquals("test-bucket", request.getContainerName());
            assertEquals("faiss", request.getEngine());
            assertEquals("float", request.getDataType()); // TODO this will be in {fp16, fp32, byte, binary}
            assertEquals("blob.knnvec", request.getVectorPath());
            assertEquals("blob.knndid", request.getDocIdPath());
            assertEquals("test-cluster", request.getTenantId());
            assertEquals(vectorValues.size(), request.getDocCount());
        }
    }
}
