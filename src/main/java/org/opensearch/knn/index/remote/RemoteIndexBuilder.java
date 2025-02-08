/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.remote;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.common.CheckedTriFunction;
import org.opensearch.common.StopWatch;
import org.opensearch.common.StreamContext;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexSettings;
import org.opensearch.knn.index.KNNSettings;
import org.opensearch.knn.index.vectorvalues.KNNVectorValues;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

import static org.opensearch.knn.index.codec.util.KNNCodecUtil.initializeVectorValues;

/**
 * This class orchestrates building vector indices. It handles uploading data to a repository, submitting a remote
 * build request, awaiting upon the build request to complete, and finally downloading the data from a repository.
 *
 * TODO: This does not implement {@link org.opensearch.knn.index.codec.nativeindex.NativeIndexBuildStrategy} as we need to use
 * multiple iterators to upload the vectors in parallel. In order to do so we need to refactor NativeIndexBuildStrategy to support a taking in a vector values supplier.
 * TODO: This should probably be an interface for which we can have specific implementations for HNSW, IVF, etc.
 */
@Log4j2
public class RemoteIndexBuilder {

    public static final String VECTOR_PATH = "vectors";
    private final Supplier<RepositoriesService> repositoriesServiceSupplier;
    private final IndexSettings indexSettings;
    static final String VECTOR_REPO_NAME = "vector-repo"; // TODO: Get this from cluster setting
    static final String VECTOR_BLOB_FILE_EXTENSION = ".knnvec";
    static final String DOC_ID_FILE_EXTENSION = ".knndid";
    static final String GRAPH_FILE_EXTENSION = ".knngraph";

    /**
     * Public constructor
     * Note: We create this per index in order to consume {@link IndexSettings} and evaluate them during build time
     *
     * @param repositoriesServiceSupplier   A supplier for {@link RepositoriesService} used for interacting with repository
     * @param indexSettings                 {@link IndexSettings}
     */
    public RemoteIndexBuilder(Supplier<RepositoriesService> repositoriesServiceSupplier, IndexSettings indexSettings) {
        this.repositoriesServiceSupplier = repositoriesServiceSupplier;
        this.indexSettings = indexSettings;
    }

    /**
     * Gets the KNN repository container from the repository service.
     * TODO: Handle repository missing exceptions
     * @return {@link org.opensearch.repositories.RepositoriesService}
     */
    private BlobStoreRepository getRepository() {
        RepositoriesService repositoriesService = repositoriesServiceSupplier.get();
        final Repository repository = repositoriesService.repository(VECTOR_REPO_NAME);
        assert repository instanceof BlobStoreRepository : "Repository should be instance of BlobStoreRepository";
        return (BlobStoreRepository) repository;
    }

    /**
     * 1. upload files, 2 trigger build, 3 download the graph, 4 write to indexoutput
     *
     * @param fieldInfo
     * @param knnVectorValuesSupplier
     * @param totalLiveDocs
     */
    public void buildIndexRemotely(
        FieldInfo fieldInfo,
        Supplier<KNNVectorValues<?>> knnVectorValuesSupplier,
        int totalLiveDocs,
        SegmentWriteState segmentWriteState
    ) {
        try {
            StopWatch stopWatch;
            long time_in_millis;

            stopWatch = new StopWatch().start();
            writeToRepository(fieldInfo, knnVectorValuesSupplier, totalLiveDocs, segmentWriteState);
            time_in_millis = stopWatch.stop().totalTime().millis();
            log.info("Repository write took {} ms for vector field [{}]", time_in_millis, fieldInfo.getName());

            stopWatch = new StopWatch().start();
            submitVectorBuild();
            time_in_millis = stopWatch.stop().totalTime().millis();
            log.info("Submit vector build took {} ms for vector field [{}]", time_in_millis, fieldInfo.getName());

            stopWatch = new StopWatch().start();
            awaitVectorBuild();
            time_in_millis = stopWatch.stop().totalTime().millis();
            log.info("Await vector build took {} ms for vector field [{}]", time_in_millis, fieldInfo.getName());

            stopWatch = new StopWatch().start();
            readFromRepository();
            time_in_millis = stopWatch.stop().totalTime().millis();
            log.info("Repository read took {} ms for vector field [{}]", time_in_millis, fieldInfo.getName());

        } catch (Exception e) {
            log.info("Remote Build Exception", e);
            e.printStackTrace();
        }
    }

    /**
     * Write relevant vector data to repository
     *
     * @param fieldInfo
     * @param knnVectorValuesSupplier
     * @param totalLiveDocs
     * @param segmentWriteState
     * @throws IOException
     * @throws InterruptedException
     */
    private void writeToRepository(
        FieldInfo fieldInfo,
        Supplier<KNNVectorValues<?>> knnVectorValuesSupplier,
        int totalLiveDocs,
        SegmentWriteState segmentWriteState
    ) throws IOException, InterruptedException {
        log.info("Writing Files To Repository");
        // TODO: Only uploading vectors for now for benchmarking purposes
        BlobContainer blobContainer = getRepository().blobStore()
            .blobContainer(getRepository().basePath().add(RemoteIndexBuilder.VECTOR_PATH));

        if (blobContainer instanceof AsyncMultiStreamBlobContainer && KNNSettings.getParallelVectorUpload()) {
            log.info("Parallel Uploads Supported");
            KNNVectorValues<?> knnVectorValues = knnVectorValuesSupplier.get();
            initializeVectorValues(knnVectorValues);

            String blobName = fieldInfo.getName() + "_" + segmentWriteState.segmentInfo.name + VECTOR_BLOB_FILE_EXTENSION;
            WriteContext writeContext = new WriteContext.Builder().fileName(blobName).streamContextSupplier((partSize) -> {
                long contentLength = (long) knnVectorValues.bytesPerVector() * totalLiveDocs;
                long lastPartSize = (contentLength % partSize) != 0 ? contentLength % partSize : partSize;
                int numberOfParts = (int) ((contentLength % partSize) == 0 ? contentLength / partSize : (contentLength / partSize) + 1);

                log.info(
                    "Performing parallel upload with partSize: {}, numberOfParts: {}, lastPartSize: {}",
                    partSize,
                    numberOfParts,
                    lastPartSize
                );
                return new StreamContext(getTransferPartStreamSupplier(knnVectorValuesSupplier), partSize, lastPartSize, numberOfParts);
            })
                .fileSize((long) knnVectorValues.bytesPerVector() * totalLiveDocs)
                .failIfAlreadyExists(false)
                .writePriority(WritePriority.NORMAL)
                .uploadFinalizer((bool) -> {})
                .doRemoteDataIntegrityCheck(false)
                .expectedChecksum(null)
                .metadata(null)
                .build();

            final CountDownLatch latch = new CountDownLatch(1);
            ((AsyncMultiStreamBlobContainer) blobContainer).asyncBlobUpload(
                writeContext,
                new LatchedActionListener<>(new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
                        log.info("Parallel Upload Completed");
                    }

                    @Override
                    public void onFailure(Exception e) {
                        log.info("Parallel Upload Failed");
                        e.printStackTrace();
                    }
                }, latch)
            );
            latch.await();
        } else {
            // Write Vectors
            InputStream vectorStream = new VectorValuesInputStream(knnVectorValuesSupplier.get());
            KNNVectorValues<?> knnVectorValues = knnVectorValuesSupplier.get();
            initializeVectorValues(knnVectorValues);
            String blobName = fieldInfo.getName() + "_" + segmentWriteState.segmentInfo.name + VECTOR_BLOB_FILE_EXTENSION;
            blobContainer.writeBlob(blobName, vectorStream, (long) knnVectorValues.bytesPerVector() * totalLiveDocs, false);
        }
    }

    private CheckedTriFunction<Integer, Long, Long, InputStreamContainer, IOException> getTransferPartStreamSupplier(
        Supplier<KNNVectorValues<?>> knnVectorValuesSupplier
    ) {
        return ((partNo, size, position) -> {
            log.info("Using InputStream with partNo: {}, size: {}, position: {}", partNo, size, position);
            VectorValuesInputStream vectorValuesInputStream = new VectorValuesInputStream(knnVectorValuesSupplier.get());
            long bytesSkipped = vectorValuesInputStream.skip(position);
            if (bytesSkipped != position) {
                throw new IllegalArgumentException("Skipped " + bytesSkipped + " bytes, expected to skip " + position);
            }
            vectorValuesInputStream.setBytesRemaining(size);
            return new InputStreamContainer(vectorValuesInputStream, size, position);
        });
    }

    /**
     * Submit vector build request to remote vector build service
     *
     */
    private void submitVectorBuild() {

    }

    /**
     * Wait on remote vector build to complete
     */
    private void awaitVectorBuild() {

    }

    /**
     * Read constructed vector file from remote repository and write to IndexOutput
     */
    private void readFromRepository() {

    }
}
