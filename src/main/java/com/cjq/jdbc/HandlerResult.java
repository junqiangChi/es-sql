package com.cjq.jdbc;


import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

/**
 * Result handler for Elasticsearch query operations
 * Encapsulates query results including headers, rows, and operation status
 * Provides methods to convert results to JSON format for different operation types
 */
public class HandlerResult {
    
    /** Column headers for the result set */
    private final List<String> headers;
    
    /** Data rows containing the query results */
    private final List<List<Object>> rows;
    
    /** Flag indicating if this is a DML (Data Manipulation Language) operation */
    private boolean isDml;
    
    /** Flag indicating if the operation was successful */
    private boolean isSuccess;
    
    /** Number of affected rows for DML operations */
    private Long number;

    /**
     * Constructs a new HandlerResult with headers and data rows
     * 
     * @param headers the column headers for the result set
     * @param lines the data rows containing the query results
     */
    public HandlerResult(List<String> headers, List<List<Object>> lines) {
        this.headers = headers;
        this.rows = lines;
    }

    /**
     * Gets the column headers for the result set
     * 
     * @return list of column header names
     */
    public List<String> getHeaders() {
        return headers;
    }

    /**
     * Gets the data rows containing the query results
     * 
     * @return list of data rows, where each row is a list of values
     */
    public List<List<Object>> getRows() {
        return rows;
    }

    /**
     * Checks if the operation was successful
     * 
     * @return true if the operation succeeded, false otherwise
     */
    public boolean isSuccess() {
        return isSuccess;
    }

    /**
     * Sets the success status of the operation
     * 
     * @param success true if the operation succeeded, false otherwise
     * @return this HandlerResult instance for method chaining
     */
    public HandlerResult setSuccess(boolean success) {
        isSuccess = success;
        return this;
    }

    /**
     * Gets the number of affected rows for DML operations
     * 
     * @return number of affected rows, or null if not applicable
     */
    public Long getNumber() {
        return number;
    }

    /**
     * Sets the number of affected rows for DML operations
     * 
     * @param number the number of affected rows
     */
    public void setNumber(Long number) {
        this.number = number;
    }

    /**
     * Sets the DML flag indicating if this is a data manipulation operation
     * 
     * @param dml true if this is a DML operation, false otherwise
     * @return this HandlerResult instance for method chaining
     */
    public HandlerResult setDml(boolean dml) {
        isDml = dml;
        return this;
    }

    /**
     * Checks if this result represents a DML operation
     * 
     * @return true if this is a DML operation, false otherwise
     */
    public boolean isDml() {
        return isDml;
    }

    /**
     * Sets both success status and number of affected rows
     * 
     * @param success true if the operation succeeded, false otherwise
     * @param number the number of affected rows
     * @return this HandlerResult instance for method chaining
     */
    public HandlerResult setSuccess(boolean success, Long number) {
        this.isSuccess = success;
        this.number = number;
        return this;
    }

    /**
     * Converts DML operation result to JSON string format
     * Includes success status and number of affected rows
     * 
     * @return JSON string representation of the DML result
     */
    public String dmlToJsonStr() {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.startObject();
            builder.field("success");
            builder.value(isSuccess);
            builder.field("effectRows");
            builder.value(number);
            builder.endObject();
            BytesReference bytesReference = BytesReference.bytes(builder);
            return bytesReference.utf8ToString();
        } catch (IOException e) {
            return "{\"success\":false,\"effectRows\":0}";
        }
    }

    /**
     * Converts query result to JSON string format
     * Includes headers and data rows in a structured format
     * 
     * @return JSON string representation of the query result
     */
    public String resultToJsonStr() {
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