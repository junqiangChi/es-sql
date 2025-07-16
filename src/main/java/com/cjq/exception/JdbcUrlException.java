package com.cjq.exception;

/**
 * Exception thrown when JDBC URL parsing or validation fails
 * Extends BaseException to provide unified error handling
 */
public class JdbcUrlException extends BaseException {
    
    /**
     * Constructs a new JDBC URL exception with default error code
     *
     * @param message the detail message
     */
    public JdbcUrlException(String message) {
        super(ErrorCode.INVALID_JDBC_URL.getCode(), message);
    }
    
    /**
     * Constructs a new JDBC URL exception with custom error code
     *
     * @param errorCode the error code
     * @param message the detail message
     */
    public JdbcUrlException(ErrorCode errorCode, String message) {
        super(errorCode.getCode(), message);
    }
    
    /**
     * Constructs a new JDBC URL exception with context
     *
     * @param message the detail message
     * @param errorContext additional context information
     */
    public JdbcUrlException(String message, String errorContext) {
        super(ErrorCode.INVALID_JDBC_URL.getCode(), message, errorContext);
    }
    
    /**
     * Constructs a new JDBC URL exception with cause
     *
     * @param message the detail message
     * @param cause the cause of the exception
     */
    public JdbcUrlException(String message, Throwable cause) {
        super(ErrorCode.INVALID_JDBC_URL.getCode(), message, cause);
    }
    
    /**
     * Constructs a new JDBC URL exception with context and cause
     *
     * @param message the detail message
     * @param errorContext additional context information
     * @param cause the cause of the exception
     */
    public JdbcUrlException(String message, String errorContext, Throwable cause) {
        super(ErrorCode.INVALID_JDBC_URL.getCode(), message, errorContext, cause);
    }
    
    /**
     * Constructs a new JDBC URL exception with custom error code and cause
     *
     * @param errorCode the error code
     * @param message the detail message
     * @param cause the cause of the exception
     */
    public JdbcUrlException(ErrorCode errorCode, String message, Throwable cause) {
        super(errorCode.getCode(), message, cause);
    }
    
    /**
     * Constructs a new JDBC URL exception with custom error code, context and cause
     *
     * @param errorCode the error code
     * @param message the detail message
     * @param errorContext additional context information
     * @param cause the cause of the exception
     */
    public JdbcUrlException(ErrorCode errorCode, String message, String errorContext, Throwable cause) {
        super(errorCode.getCode(), message, errorContext, cause);
    }
}
