package com.cjq.exception;

/**
 * Exception thrown when Elasticsearch query execution fails
 * Extends BaseException to provide unified error handling
 */
public class ElasticsearchExecuteException extends BaseException {
    
    /**
     * Constructs a new Elasticsearch execute exception with default error code
     *
     * @param message the detail message
     */
    public ElasticsearchExecuteException(String message) {
        super(ErrorCode.EXECUTION_FAILED.getCode(), message);
    }
    
    /**
     * Constructs a new Elasticsearch execute exception with custom error code
     *
     * @param errorCode the error code
     * @param message the detail message
     */
    public ElasticsearchExecuteException(ErrorCode errorCode, String message) {
        super(errorCode.getCode(), message);
    }
    
    /**
     * Constructs a new Elasticsearch execute exception with context
     *
     * @param message the detail message
     * @param errorContext additional context information
     */
    public ElasticsearchExecuteException(String message, String errorContext) {
        super(ErrorCode.EXECUTION_FAILED.getCode(), message, errorContext);
    }
    
    /**
     * Constructs a new Elasticsearch execute exception with cause
     *
     * @param message the detail message
     * @param cause the cause of the exception
     */
    public ElasticsearchExecuteException(String message, Throwable cause) {
        super(ErrorCode.EXECUTION_FAILED.getCode(), message, cause);
    }
    
    /**
     * Constructs a new Elasticsearch execute exception with context and cause
     *
     * @param message the detail message
     * @param errorContext additional context information
     * @param cause the cause of the exception
     */
    public ElasticsearchExecuteException(String message, String errorContext, Throwable cause) {
        super(ErrorCode.EXECUTION_FAILED.getCode(), message, errorContext, cause);
    }
    
    /**
     * Constructs a new Elasticsearch execute exception with custom error code and cause
     *
     * @param errorCode the error code
     * @param message the detail message
     * @param cause the cause of the exception
     */
    public ElasticsearchExecuteException(ErrorCode errorCode, String message, Throwable cause) {
        super(errorCode.getCode(), message, cause);
    }
    
    /**
     * Constructs a new Elasticsearch execute exception with custom error code, context and cause
     *
     * @param errorCode the error code
     * @param message the detail message
     * @param errorContext additional context information
     * @param cause the cause of the exception
     */
    public ElasticsearchExecuteException(ErrorCode errorCode, String message, String errorContext, Throwable cause) {
        super(errorCode.getCode(), message, errorContext, cause);
    }
}
