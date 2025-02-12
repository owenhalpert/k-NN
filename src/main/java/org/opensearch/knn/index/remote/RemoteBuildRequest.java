/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.remote;

import org.opensearch.common.xcontent.json.JsonXContent;
import lombok.Builder;
import lombok.Getter;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Builder
@Getter
public class RemoteBuildRequest {
    private final String repositoryType;
    private final String containerName;
    private final String objectPath;
    private final String tenantId;
    private final int dimension;
    private final int docCount;
    private final String dataType;
    private final String engine;
    private final String algorithm;
    @Builder.Default
    private final Map<String, Object> indexParameters = new HashMap<>();

    // TODO: Add type checking to all parameters, add individual methods (e.g. setEfConstruction) to check index params

    public String toJson() throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.field("repository_type", repositoryType);
            builder.field("container_name", containerName);
            builder.field("object_path", objectPath);
            builder.field("tenant_id", tenantId);
            builder.field("dimension", dimension);
            builder.field("doc_count", docCount);
            builder.field("data_type", dataType);
            builder.field("engine", engine);
            builder.field("index_parameters", indexParameters);
            builder.endObject();
            return builder.toString();
        }
    }

}
