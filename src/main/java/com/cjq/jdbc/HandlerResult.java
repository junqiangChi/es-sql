package com.cjq.jdbc;


import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

public class HandlerResult {
    private final List<String> headers;
    private final List<List<Object>> rows;
    private boolean isSuccess;


    public HandlerResult(List<String> headers, List<List<Object>> lines) {
        this.headers = headers;
        this.rows = lines;
    }

    public List<String> getHeaders() {
        return headers;
    }

    public List<List<Object>> getRows() {
        return rows;
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    public HandlerResult setSuccess(boolean success) {
        isSuccess = success;
        return this;
    }

    public String toJsonString() {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.startObject();
            builder.field("headers");
            builder.startArray();
            if (headers != null) {
                for (String header : headers) {
                    builder.value(header);
                }
            }
            builder.endArray();

            builder.field("rows");
            builder.startArray();
            if (rows != null) {
                for (List<Object> row : rows) {
                    builder.startArray();
                    if (row != null) {
                        for (Object value : row) {
                            builder.value(value);
                        }
                    }
                    builder.endArray();
                }
            }
            builder.endArray();
            builder.endObject();
            BytesReference bytesReference = BytesReference.bytes(builder);
            return bytesReference.utf8ToString();
        } catch (IOException e) {
            return "{\"headers\":[],\"rows\":[]}";
        }
    }
}