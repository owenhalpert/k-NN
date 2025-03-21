/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.nativeindex.remote;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang.StringUtils;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.common.CheckedTriFunction;
import org.opensearch.common.StopWatch;
import org.opensearch.common.StreamContext;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.core.action.ActionListener;
import org.opensearch.knn.index.KNNSettings;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.engine.KNNEngine;
import org.opensearch.knn.index.store.IndexOutputWithBuffer;
import org.opensearch.knn.index.vectorvalues.KNNVectorValues;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.opensearch.knn.common.KNNConstants.DOC_ID_FILE_EXTENSION;
import static org.opensearch.knn.common.KNNConstants.VECTOR_BLOB_FILE_EXTENSION;
import static org.opensearch.knn.index.KNNSettings.state;
import static org.opensearch.knn.index.codec.util.KNNCodecUtil.initializeVectorValues;

@Log4j2
@AllArgsConstructor
public class DefaultVectorRepositoryAccessor implements VectorRepositoryAccessor {
    private final BlobContainer blobContainer;

    /**
     * If the repository implements {@link AsyncMultiStreamBlobContainer}, then parallel uploads will be used. Parallel uploads are backed by a {@link WriteContext}, for which we have a custom
     * {@link org.opensearch.common.blobstore.stream.write.StreamContextSupplier} implementation.
     *
     * @see DefaultVectorRepositoryAccessor#getStreamContext
     * @see DefaultVectorRepositoryAccessor#getTransferPartStreamSupplier
     *
     * @param blobName                  Base name of the blobs we are writing, excluding file extensions
     * @param totalLiveDocs             Number of documents we are processing. This is used to compute the size of the blob we are writing
     * @param vectorDataType            Data type of the vector (FLOAT, BYTE, BINARY)
     * @param knnVectorValuesSupplier   Supplier for {@link KNNVectorValues}
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void writeToRepository(
        String blobName,
        int totalLiveDocs,
        VectorDataType vectorDataType,
        Supplier<KNNVectorValues<?>> knnVectorValuesSupplier
    ) throws IOException, InterruptedException {
        int bufferSize = (int) state().getUploadBufferSize().getBytes();
        boolean forceSingleStream = state().getForceSingleStreamUpload();
        assert blobContainer != null;
        KNNVectorValues<?> knnVectorValues = knnVectorValuesSupplier.get();
        initializeVectorValues(knnVectorValues);
        long vectorBlobLength = (long) knnVectorValues.bytesPerVector() * totalLiveDocs;

        if (blobContainer instanceof AsyncMultiStreamBlobContainer asyncBlobContainer && !forceSingleStream) {
            // First initiate vectors upload
            log.debug("Container {} Supports Parallel Blob Upload", blobContainer);
            // WriteContext is the main entry point into asyncBlobUpload. It stores all of our upload configurations, analogous to
            // BuildIndexParams
            WriteContext writeContext = createWriteContext(blobName, vectorBlobLength, knnVectorValuesSupplier, vectorDataType);

            AtomicReference<Exception> exception = new AtomicReference<>();
            final CountDownLatch latch = new CountDownLatch(1);
            asyncBlobContainer.asyncBlobUpload(writeContext, new LatchedActionListener<>(new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    log.debug(
                        "Parallel vector upload succeeded for blob {} with size {}",
                        blobName + VECTOR_BLOB_FILE_EXTENSION,
                        vectorBlobLength
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    log.debug(
                        "Parallel vector upload failed for blob {} with size {}",
                        blobName + VECTOR_BLOB_FILE_EXTENSION,
                        vectorBlobLength,
                        e
                    );
                    exception.set(e);
                }
            }, latch));

            // Then upload doc id blob before waiting on vector uploads
            // TODO: We wrap with a BufferedInputStream to support retries. We can tune this buffer size to optimize performance.
            // Note: We do not use the parallel upload API here as the doc id blob will be much smaller than the vector blob
            writeDocIds(knnVectorValuesSupplier.get(), vectorBlobLength, totalLiveDocs, blobName, blobContainer, bufferSize);
            latch.await();
            if (exception.get() != null) {
                throw new IOException(exception.get());
            }
        } else {
            if (forceSingleStream) {
                log.debug("Using single stream upload because knn.force.single.stream.upload is true");
            } else {
                log.debug("Container {} Does Not Support Parallel Blob Upload", blobContainer);
            }

            StopWatch vectorStopWatch = new StopWatch().start();
            Runtime runtime = Runtime.getRuntime();
            long initialUsedMemory = runtime.totalMemory() - runtime.freeMemory();

            try (
                InputStream vectorStream = new BufferedInputStream(
                    new VectorValuesInputStream(knnVectorValuesSupplier.get(), vectorDataType),
                    bufferSize
                )
            ) {
                log.debug(
                    "Writing {} bytes for {} docs to {} with buffer size {}",
                    vectorBlobLength,
                    totalLiveDocs,
                    blobName + VECTOR_BLOB_FILE_EXTENSION,
                    bufferSize
                );
                blobContainer.writeBlob(blobName + VECTOR_BLOB_FILE_EXTENSION, vectorStream, vectorBlobLength, true);
            }

            long vectorTime = vectorStopWatch.stop().totalTime().millis();
            long finalUsedMemory = runtime.totalMemory() - runtime.freeMemory();
            double vectorThroughputMBps = (vectorBlobLength / (1024.0 * 1024.0)) / (vectorTime / 1000.0);

            log.info(
                "Vector write metrics: Buffer Size: {} bytes, Total Bytes: {} bytes, Time: {} ms, "
                    + "Throughput: {} MB/s, Vectors Written: {}, Bytes per Vector: {} bytes, "
                    + "Memory Delta: {} MB",
                bufferSize,
                vectorBlobLength,
                vectorTime,
                String.format("%.2f", vectorThroughputMBps),
                totalLiveDocs,
                knnVectorValues.bytesPerVector(),
                String.format("%.2f", (finalUsedMemory - initialUsedMemory) / (1024.0 * 1024.0))
            );

            StopWatch docIdStopWatch = new StopWatch().start();
            initialUsedMemory = runtime.totalMemory() - runtime.freeMemory();

            writeDocIds(knnVectorValuesSupplier.get(), vectorBlobLength, totalLiveDocs, blobName, blobContainer, bufferSize);

            long docIdTime = docIdStopWatch.stop().totalTime().millis();
            finalUsedMemory = runtime.totalMemory() - runtime.freeMemory();
            long docIdBytes = (long) totalLiveDocs * Integer.BYTES;
            double docIdThroughputMBps = (docIdBytes / (1024.0 * 1024.0)) / (docIdTime / 1000.0);

            log.info(
                "Doc ID write metrics: Buffer Size: {} bytes, Total Bytes: {} bytes, Time: {} ms, "
                    + "Throughput: {} MB/s, Doc IDs Written: {}, Memory Delta: {} KB",
                bufferSize,
                docIdBytes,
                docIdTime,
                String.format("%.2f", docIdThroughputMBps),
                totalLiveDocs,
                String.format("%.2f", (finalUsedMemory - initialUsedMemory) / 1024.0)
            );
        }
    }

    /**
     * Helper method for uploading doc ids to repository, as it's re-used in both parallel and sequential upload cases
     * @param knnVectorValues
     * @param vectorBlobLength
     * @param totalLiveDocs
     * @param blobName
     * @param blobContainer
     * @throws IOException
     */
    private void writeDocIds(
        KNNVectorValues<?> knnVectorValues,
        long vectorBlobLength,
        long totalLiveDocs,
        String blobName,
        BlobContainer blobContainer,
        int bufferSize
    ) throws IOException {
        try (InputStream docStream = new BufferedInputStream(new DocIdInputStream(knnVectorValues), bufferSize)) {
            log.debug(
                "Doc ID write: Writing {} bytes for {} docs ids to {} with buffer size {}",
                totalLiveDocs * Integer.BYTES,
                totalLiveDocs,
                blobName + DOC_ID_FILE_EXTENSION,
                bufferSize
            );
            blobContainer.writeBlob(blobName + DOC_ID_FILE_EXTENSION, docStream, totalLiveDocs * Integer.BYTES, true);
        }
    }

    /**
     * Returns a {@link org.opensearch.common.StreamContext}. Intended to be invoked as a {@link org.opensearch.common.blobstore.stream.write.StreamContextSupplier},
     * which takes the partSize determined by the repository implementation and calculates the number of parts as well as handles the last part of the stream.
     *
     * @see DefaultVectorRepositoryAccessor#getTransferPartStreamSupplier
     *
     * @param partSize                  Size of each InputStream to be uploaded in parallel. Provided by repository implementation
     * @param vectorBlobLength          Total size of the vectors across all InputStreams
     * @param knnVectorValuesSupplier   Supplier for {@link KNNVectorValues}
     * @param vectorDataType            Data type of the vector (FLOAT, BYTE, BINARY)
     * @return a {@link org.opensearch.common.StreamContext} with a function that will create {@link InputStream}s of {@param partSize}
     */
    private StreamContext getStreamContext(
        long partSize,
        long vectorBlobLength,
        Supplier<KNNVectorValues<?>> knnVectorValuesSupplier,
        VectorDataType vectorDataType
    ) {
        long lastPartSize = (vectorBlobLength % partSize) != 0 ? vectorBlobLength % partSize : partSize;
        int numberOfParts = (int) ((vectorBlobLength % partSize) == 0 ? vectorBlobLength / partSize : (vectorBlobLength / partSize) + 1);
        return new StreamContext(
            getTransferPartStreamSupplier(knnVectorValuesSupplier, vectorDataType),
            partSize,
            lastPartSize,
            numberOfParts
        );
    }

    /**
     * This method handles creating {@link VectorValuesInputStream}s based on the part number, the requested size of the stream part, and the position that the stream starts at within the underlying {@link KNNVectorValues}
     *
     * @param knnVectorValuesSupplier       Supplier for {@link KNNVectorValues}
     * @param vectorDataType                Data type of the vector (FLOAT, BYTE, BINARY)
     * @return a function with which the repository implementation will use to create {@link VectorValuesInputStream}s of specific sizes and start positions.
     */
    private CheckedTriFunction<Integer, Long, Long, InputStreamContainer, IOException> getTransferPartStreamSupplier(
        Supplier<KNNVectorValues<?>> knnVectorValuesSupplier,
        VectorDataType vectorDataType
    ) {
        return ((partNo, size, position) -> {
            log.debug("Creating InputStream for partNo: {}, size: {}, position: {}", partNo, size, position);
            VectorValuesInputStream vectorValuesInputStream = new VectorValuesInputStream(
                knnVectorValuesSupplier.get(),
                vectorDataType,
                position,
                size
            );
            return new InputStreamContainer(vectorValuesInputStream, size, position);
        });
    }

    /**
     * Creates a {@link WriteContext} meant to be used by {@link AsyncMultiStreamBlobContainer#asyncBlobUpload}.
     * Note: Integrity checking is left up to the vendor repository and SDK implementations.
     * @param blobName
     * @param vectorBlobLength
     * @param knnVectorValuesSupplier
     * @param vectorDataType
     * @return
     */
    private WriteContext createWriteContext(
        String blobName,
        long vectorBlobLength,
        Supplier<KNNVectorValues<?>> knnVectorValuesSupplier,
        VectorDataType vectorDataType
    ) {
        return new WriteContext.Builder().fileName(blobName + VECTOR_BLOB_FILE_EXTENSION)
            .streamContextSupplier((partSize) -> getStreamContext(partSize, vectorBlobLength, knnVectorValuesSupplier, vectorDataType))
            .fileSize(vectorBlobLength)
            .failIfAlreadyExists(true)
            .writePriority(WritePriority.NORMAL)
            .uploadFinalizer((bool) -> {})
            .build();
    }

    @Override
    public void readFromRepository(String fileName, IndexOutputWithBuffer indexOutputWithBuffer) throws IOException {
        if (StringUtils.isBlank(fileName)) {
            throw new IllegalArgumentException("download path is null or empty");
        }
        if (!fileName.endsWith(KNNEngine.FAISS.getExtension())) {
            log.error("file name [{}] does not end with extension [{}}", fileName, KNNEngine.FAISS.getExtension());
            throw new IllegalArgumentException("download path has incorrect file extension");
        }

        // TODO: We are using the sequential download API as multi-part parallel download is difficult for us to implement today and
        // requires some changes in core. For more details, see: https://github.com/opensearch-project/k-NN/issues/2464
        Runtime runtime = Runtime.getRuntime();
        StopWatch readStopwatch = new StopWatch().start();
        long initialUsedMemory = runtime.totalMemory() - runtime.freeMemory();

        Map<String, BlobMetadata> blobMetadataMap = blobContainer.listBlobsByPrefix(fileName);
        BlobMetadata metadata = blobMetadataMap.get(fileName);
        long fileSize = metadata != null ? metadata.length() : -1;

        InputStream graphStream = blobContainer.readBlob(fileName);

        indexOutputWithBuffer.writeFromStreamWithBuffer(graphStream);

        long readTime = readStopwatch.stop().totalTime().millis();
        long finalUsedMemory = runtime.totalMemory() - runtime.freeMemory();
        double throughputMBps = (fileSize / (1024.0 * 1024.0)) / (readTime / 1000.0);

        log.info(
            "Repository read metrics: Buffer Size: {} bytes, Total Bytes: {} bytes, Time: {} ms, "
                + "Throughput: {} MB/s, Memory Delta: {} MB",
            KNNSettings.getDownloadBufferSize().getBytes(),
            fileSize,
            readTime,
            String.format("%.2f", throughputMBps),
            String.format("%.2f", (finalUsedMemory - initialUsedMemory) / (1024.0 * 1024.0))
        );
    }
}
