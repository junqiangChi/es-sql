package com.cjq.jdbc;


import java.util.List;

public class ObjectResult {
    private final List<String> headers;
    private final List<List<Object>> rows;
    private boolean isSuccess;


    public ObjectResult(List<String> headers, List<List<Object>> lines) {
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

    public ObjectResult setSuccess(boolean success) {
        isSuccess = success;
        return this;
    }
}