/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.remote;

import com.fasterxml.jackson.annotation.JsonValue;
import lombok.Builder;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

@Builder
@Getter
public class RemoteBuildRequest {
    private final String repositoryType;
    private final String repositoryName;
    private final String objectPath;
    private final String tenantId;
    private final int dimension;
    private final int docCount;
    private final String dataType;
    @Builder.Default
    private static final Map<String, Object> indexParameters = new HashMap<>();

    public static class RemoteBuildRequestBuilder {
        public RemoteBuildRequestBuilder addIndexParameter(String key, Object value) {
            indexParameters.put(key, value);
            return this;
        }

        public RemoteBuildRequestBuilder setMethod(String name, Map<String, Object> parameters) {
            Map<String, Object> methodConfig = new HashMap<>();
            methodConfig.put("name", name);
            methodConfig.putAll(parameters);
            indexParameters.put("method", methodConfig);
            return this;
        }
    }

    // TODO: Add type checking to index parameters by creating a map and looking up data type in that map?

    @JsonValue
    public Map<String, Object> toJson() {
        Map<String, Object> json = new HashMap<>();
        json.put("repository_type", repositoryType);
        json.put("repository_name", repositoryName);
        json.put("object_path", objectPath);
        json.put("tenant_id", tenantId);
        json.put("dimension", dimension);
        json.put("doc_count", docCount);
        json.put("data_type", dataType);
        json.put("index_parameters", indexParameters);
        return json;
    }
}
