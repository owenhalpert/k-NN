/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.store;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.knn.index.KNNSettings;

import java.io.IOException;
import java.io.InputStream;

/**
 * Wrapper around {@link IndexOutput} to perform writes in a buffered manner. This class is created per flush/merge, and may be used twice if
 * {@link org.opensearch.knn.index.codec.nativeindex.remote.RemoteIndexBuildStrategy} needs to fall back to a different build strategy.
 */
@Log4j2
public class IndexOutputWithBuffer {
    // Underlying `IndexOutput` obtained from Lucene's Directory.
    private IndexOutput indexOutput;
    // Write buffer. Native engine will copy bytes into this buffer.
    // Allocating 64KB here since it show better performance in NMSLIB with the size. (We had slightly improvement in FAISS than having 4KB)
    // NMSLIB writes an adjacent list size first, then followed by serializing the list. Since we usually have more adjacent lists, having
    // 64KB to accumulate bytes as possible to reduce the times of calling `writeBytes`.
    // private static final int CHUNK_SIZE = 64 * 1024;
    private final byte[] buffer;

    public IndexOutputWithBuffer(IndexOutput indexOutput) {
        this.indexOutput = indexOutput;
        int bufferSize = (int) KNNSettings.getDownloadBufferSize().getBytes();
        this.buffer = new byte[bufferSize];
    }

    // This method will be called in JNI layer which precisely knows
    // the amount of bytes need to be written.
    public void writeBytes(int length) {
        try {
            // Delegate Lucene `indexOuptut` to write bytes.
            indexOutput.writeBytes(buffer, 0, length);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Writes to the {@link IndexOutput} by buffering bytes into the existing buffer in this class.
     *
     * @param inputStream       The stream from which we are reading bytes to write
     * @throws IOException
     * @see IndexOutputWithBuffer#writeFromStreamWithBuffer(InputStream, byte[])
     */
    public void writeFromStreamWithBuffer(InputStream inputStream) throws IOException {
        log.info("Writing from stream to index output with buffer size: {}", this.buffer.length);
        writeFromStreamWithBuffer(inputStream, this.buffer);
    }

    /**
     * Writes to the {@link IndexOutput} by buffering bytes with @param outputBuffer. This method allows
     * {@link org.opensearch.knn.index.codec.nativeindex.remote.RemoteIndexBuildStrategy} to provide a separate, larger buffer as that buffer is for buffering
     * bytes downloaded from the repository, so it may be more performant to use a larger buffer.
     * We do not change the size of the existing buffer in case a fallback to the existing build strategy is needed.
     * TODO: Tune the size of the buffer used by RemoteIndexBuildStrategy based on benchmarking
     *
     * @param inputStream       The stream from which we are reading bytes to write
     * @param outputBuffer      The buffer used to buffer bytes
     * @throws IOException
     * @see IndexOutputWithBuffer#writeFromStreamWithBuffer(InputStream)
     */
    private void writeFromStreamWithBuffer(InputStream inputStream, byte[] outputBuffer) throws IOException {
        int bytesRead = 0;
        // InputStream uses -1 indicates there are no more bytes to be read
        while (bytesRead != -1) {
            // Try to read CHUNK_SIZE into the buffer. The actual amount read may be less.
            bytesRead = inputStream.read(outputBuffer, 0, this.buffer.length);
            assert bytesRead <= this.buffer.length;
            // However many bytes we read, write it to the IndexOutput if != -1
            if (bytesRead != -1) {
                indexOutput.writeBytes(outputBuffer, 0, bytesRead);
            }
        }
    }

    @Override
    public String toString() {
        return "{indexOutput=" + indexOutput + ", len(buffer)=" + buffer.length + "}";
    }
}
