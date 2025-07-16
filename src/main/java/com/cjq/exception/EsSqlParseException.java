package com.cjq.exception;

/**
 * Exception thrown when SQL parsing fails
 * Extends BaseException to provide unified error handling
 */
public class EsSqlParseException extends BaseException {
    
    /**
     * Constructs a new SQL parse exception with default error code
     *
     * @param message the detail message
     */
    public EsSqlParseException(String message) {
        super(ErrorCode.SQL_PARSE_ERROR.getCode(), message);
    }
    
    /**
     * Constructs a new SQL parse exception with custom error code
     *
     * @param errorCode the error code
     * @param message the detail message
     */
    public EsSqlParseException(ErrorCode errorCode, String message) {
        super(errorCode.getCode(), message);
    }
    
    /**
     * Constructs a new SQL parse exception with context
     *
     * @param message the detail message
     * @param errorContext additional context information
     */
    public EsSqlParseException(String message, String errorContext) {
        super(ErrorCode.SQL_PARSE_ERROR.getCode(), message, errorContext);
    }
    
    /**
     * Constructs a new SQL parse exception with cause
     *
     * @param message the detail message
     * @param cause the cause of the exception
     */
    public EsSqlParseException(String message, Throwable cause) {
        super(ErrorCode.SQL_PARSE_ERROR.getCode(), message, cause);
    }
    
    /**
     * Constructs a new SQL parse exception with context and cause
     *
     * @param message the detail message
     * @param errorContext additional context information
     * @param cause the cause of the exception
     */
    public EsSqlParseException(String message, String errorContext, Throwable cause) {
        super(ErrorCode.SQL_PARSE_ERROR.getCode(), message, errorContext, cause);
    }
    
    /**
     * Constructs a new SQL parse exception with custom error code and cause
     *
     * @param errorCode the error code
     * @param message the detail message
     * @param cause the cause of the exception
     */
    public EsSqlParseException(ErrorCode errorCode, String message, Throwable cause) {
        super(errorCode.getCode(), message, cause);
    }
    
    /**
     * Constructs a new SQL parse exception with custom error code, context and cause
     *
     * @param errorCode the error code
     * @param message the detail message
     * @param errorContext additional context information
     * @param cause the cause of the exception
     */
    public EsSqlParseException(ErrorCode errorCode, String message, String errorContext, Throwable cause) {
        super(errorCode.getCode(), message, errorContext, cause);
    }
}
