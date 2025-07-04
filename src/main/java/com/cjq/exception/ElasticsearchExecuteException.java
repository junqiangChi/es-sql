package com.cjq.exception;

public class ElasticsearchExecuteException extends RuntimeException {
    public ElasticsearchExecuteException(String message) {
        super(message);
    }

    public ElasticsearchExecuteException() {
        super();
    }

    public ElasticsearchExecuteException(String message, Throwable cause) {
        super(message, cause);
    }

    public ElasticsearchExecuteException(Throwable cause) {
        super(cause);
    }

    protected ElasticsearchExecuteException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
