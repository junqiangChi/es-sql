package com.cjq.exception;

/**
 * Base exception class for ES SQL project
 * Provides unified exception handling foundation with error codes and context information
 */
public abstract class BaseException extends RuntimeException {
    
    private final String errorCode;
    private final String errorContext;
    private final long timestamp;
    
    /**
     * Constructs a new base exception with error code and message
     *
     * @param errorCode the error code for this exception
     * @param message the detail message
     */
    protected BaseException(String errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
        this.errorContext = null;
        this.timestamp = System.currentTimeMillis();
    }
    
    /**
     * Constructs a new base exception with error code, message and context
     *
     * @param errorCode the error code for this exception
     * @param message the detail message
     * @param errorContext additional context information
     */
    protected BaseException(String errorCode, String message, String errorContext) {
        super(message);
        this.errorCode = errorCode;
        this.errorContext = errorContext;
        this.timestamp = System.currentTimeMillis();
    }
    
    /**
     * Constructs a new base exception with error code, message and cause
     *
     * @param errorCode the error code for this exception
     * @param message the detail message
     * @param cause the cause of the exception
     */
    protected BaseException(String errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.errorContext = null;
        this.timestamp = System.currentTimeMillis();
    }
    
    /**
     * Constructs a new base exception with error code, message, context and cause
     *
     * @param errorCode the error code for this exception
     * @param message the detail message
     * @param errorContext additional context information
     * @param cause the cause of the exception
     */
    protected BaseException(String errorCode, String message, String errorContext, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.errorContext = errorContext;
        this.timestamp = System.currentTimeMillis();
    }
    
    /**
     * Gets the error code for this exception
     *
     * @return the error code
     */
    public String getErrorCode() {
        return errorCode;
    }
    
    /**
     * Gets the error context information
     *
     * @return the error context, or null if not provided
     */
    public String getErrorContext() {
        return errorContext;
    }
    
    /**
     * Gets the timestamp when this exception was created
     *
     * @return the timestamp in milliseconds
     */
    public long getTimestamp() {
        return timestamp;
    }
    
    /**
     * Gets a formatted error message including error code and context
     *
     * @return formatted error message
     */
    public String getFormattedMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("Error Code: ").append(errorCode);
        sb.append(", Message: ").append(getMessage());
        
        if (errorContext != null) {
            sb.append(", Context: ").append(errorContext);
        }
        
        if (getCause() != null) {
            sb.append(", Cause: ").append(getCause().getMessage());
        }
        
        return sb.toString();
    }
    
    @Override
    public String toString() {
        return getClass().getSimpleName() + " [" + getFormattedMessage() + "]";
    }
} 