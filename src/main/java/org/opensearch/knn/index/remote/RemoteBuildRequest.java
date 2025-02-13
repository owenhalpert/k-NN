/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.remote;

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.HashMap;
import java.util.Map;

public class RemoteBuildRequest {
    private final String repositoryType;
    private final String repositoryName;
    private final String objectPath;
    private final String tenantId;
    private final String numDocs;
    private final String vector_data_type;
    private final Map<String, Object> indexParameters;

    public static class Builder {
        private String repositoryType;
        private String repositoryName;
        private String objectPath;
        private String tenantId;
        private String numDocs;
        private String vector_data_type;
        private Map<String, Object> indexParameters = new HashMap<>();

        public Builder repositoryType(String repositoryType) {
            this.repositoryType = repositoryType;
            return this;
        }

        public Builder repositoryName(String repositoryName) {
            this.repositoryName = repositoryName;
            return this;
        }

        public Builder objectPath(String objectPath) {
            this.objectPath = objectPath;
            return this;
        }

        public Builder tenantId(String tenantId) {
            this.tenantId = tenantId;
            return this;
        }

        public Builder numDocs(String numDocs) {
            this.numDocs = numDocs;
            return this;
        }

        public Builder vectorDataType(String vector_data_type) {
            this.vector_data_type = vector_data_type;
            return this;
        }

        public Builder addIndexParameter(String key, Object value) {
            this.indexParameters.put(key, value);
            return this;
        }

        public Builder setMethod(String name, Map<String, Object> parameters) {
            Map<String, Object> methodConfig = new HashMap<>();
            methodConfig.put("name", name);
            methodConfig.putAll(parameters);
            this.indexParameters.put("method", methodConfig);
            return this;
        }

        public RemoteBuildRequest build() {
            return new RemoteBuildRequest(this);
        }
    }

    public RemoteBuildRequest(Builder builder) {
        this.repositoryType = builder.repositoryType;
        this.repositoryName = builder.repositoryName;
        this.objectPath = builder.objectPath;
        this.tenantId = builder.tenantId;
        this.numDocs = builder.numDocs;
        this.vector_data_type = builder.vector_data_type;
        this.indexParameters = new HashMap<>(builder.indexParameters);
    }

    @JsonValue
    public Map<String, Object> toJson() {
        Map<String, Object> json = new HashMap<>();
        json.put("repository_type", repositoryType);
        json.put("repository_name", repositoryName);
        json.put("object_path", objectPath);
        json.put("tenant_id", tenantId);
        json.put("num_docs", numDocs);
        json.put("vector_data_type", vector_data_type);
        json.put("index_parameters", indexParameters);
        return json;
    }
}
