package com.cjq.exception;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Unified exception handler for ES SQL project
 * Provides centralized exception handling, logging, and error reporting
 */
public class ExceptionHandler {
    
    private static final Logger LOGGER = Logger.getLogger(ExceptionHandler.class.getName());
    private static final ExceptionHandler INSTANCE = new ExceptionHandler();
    
    private final ConcurrentMap<String, Integer> errorCounts = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Long> lastErrorTimes = new ConcurrentHashMap<>();
    
    // Error threshold configuration
    private static final int ERROR_THRESHOLD = 10;
    private static final long ERROR_WINDOW_MS = 60000; // 1 minute
    
    private ExceptionHandler() {
        // Private constructor for singleton
    }
    
    /**
     * Gets the singleton instance of ExceptionHandler
     *
     * @return the ExceptionHandler instance
     */
    public static ExceptionHandler getInstance() {
        return INSTANCE;
    }
    
    /**
     * Handles an exception with default logging level
     *
     * @param exception the exception to handle
     */
    public void handleException(Exception exception) {
        handleException(exception, Level.SEVERE);
    }
    
    /**
     * Handles an exception with specified logging level
     *
     * @param exception the exception to handle
     * @param level the logging level
     */
    public void handleException(Exception exception, Level level) {
        if (exception == null) {
            return;
        }
        
        String errorCode = getErrorCode(exception);
        String errorMessage = getErrorMessage(exception);
        String errorContext = getErrorContext(exception);
        
        // Log the exception
        logException(exception, errorCode, errorMessage, errorContext, level);
        
        // Track error statistics
        trackError(errorCode);
        
        // Check for error threshold
        checkErrorThreshold(errorCode);
    }
    
    /**
     * Handles a BaseException with enhanced error information
     *
     * @param exception the BaseException to handle
     */
    public void handleBaseException(BaseException exception) {
        if (exception == null) {
            return;
        }
        
        String errorCode = exception.getErrorCode();
        String errorMessage = exception.getMessage();
        String errorContext = exception.getErrorContext();
        
        // Log with enhanced information
        LOGGER.log(Level.SEVERE, "BaseException occurred: {0}", exception.getFormattedMessage());
        
        if (errorContext != null) {
            LOGGER.log(Level.INFO, "Error context: {0}", errorContext);
        }
        
        // Track error statistics
        trackError(errorCode);
        
        // Check for error threshold
        checkErrorThreshold(errorCode);
    }
    
    /**
     * Creates a BaseException from a regular exception
     *
     * @param exception the original exception
     * @param errorCode the error code to use
     * @return a new BaseException
     */
    public BaseException createBaseException(Exception exception, ErrorCode errorCode) {
        return createBaseException(exception, errorCode, null);
    }
    
    /**
     * Creates a BaseException from a regular exception with context
     *
     * @param exception the original exception
     * @param errorCode the error code to use
     * @param context additional context information
     * @return a new BaseException
     */
    public BaseException createBaseException(Exception exception, ErrorCode errorCode, String context) {
        String message = exception != null ? exception.getMessage() : errorCode.getDefaultMessage();
        return new BaseException(errorCode.getCode(), message, context, exception) {};
    }
    
    /**
     * Gets error statistics for monitoring
     *
     * @return a copy of the error counts map
     */
    public ConcurrentMap<String, Integer> getErrorStatistics() {
        return new ConcurrentHashMap<>(errorCounts);
    }
    
    /**
     * Resets error statistics
     */
    public void resetErrorStatistics() {
        errorCounts.clear();
        lastErrorTimes.clear();
    }
    
    /**
     * Extracts error code from exception
     *
     * @param exception the exception
     * @return the error code
     */
    private String getErrorCode(Exception exception) {
        if (exception instanceof BaseException) {
            return ((BaseException) exception).getErrorCode();
        }
        return ErrorCode.UNEXPECTED_ERROR.getCode();
    }
    
    /**
     * Extracts error message from exception
     *
     * @param exception the exception
     * @return the error message
     */
    private String getErrorMessage(Exception exception) {
        if (exception instanceof BaseException) {
            return ((BaseException) exception).getFormattedMessage();
        }
        return exception.getMessage() != null ? exception.getMessage() : exception.getClass().getSimpleName();
    }
    
    /**
     * Extracts error context from exception
     *
     * @param exception the exception
     * @return the error context
     */
    private String getErrorContext(Exception exception) {
        if (exception instanceof BaseException) {
            return ((BaseException) exception).getErrorContext();
        }
        return null;
    }
    
    /**
     * Logs the exception with detailed information
     *
     * @param exception the exception
     * @param errorCode the error code
     * @param errorMessage the error message
     * @param errorContext the error context
     * @param level the logging level
     */
    private void logException(Exception exception, String errorCode, String errorMessage, String errorContext, Level level) {
        StringBuilder logMessage = new StringBuilder();
        logMessage.append("Exception occurred - Code: ").append(errorCode);
        logMessage.append(", Message: ").append(errorMessage);
        
        if (errorContext != null) {
            logMessage.append(", Context: ").append(errorContext);
        }
        
        LOGGER.log(level, logMessage.toString(), exception);
    }
    
    /**
     * Tracks error statistics for monitoring
     *
     * @param errorCode the error code to track
     */
    private void trackError(String errorCode) {
        long currentTime = System.currentTimeMillis();
        
        // Update error count
        errorCounts.merge(errorCode, 1, Integer::sum);
        
        // Update last error time
        lastErrorTimes.put(errorCode, currentTime);
    }
    
    /**
     * Checks if error threshold has been exceeded
     *
     * @param errorCode the error code to check
     */
    private void checkErrorThreshold(String errorCode) {
        long currentTime = System.currentTimeMillis();
        Long lastErrorTime = lastErrorTimes.get(errorCode);
        
        if (lastErrorTime != null) {
            long timeSinceLastError = currentTime - lastErrorTime;
            
            // If within error window, check threshold
            if (timeSinceLastError < ERROR_WINDOW_MS) {
                Integer errorCount = errorCounts.get(errorCode);
                if (errorCount != null && errorCount >= ERROR_THRESHOLD) {
                    LOGGER.log(Level.WARNING, 
                        "Error threshold exceeded for code {0}: {1} errors in {2}ms", 
                        new Object[]{errorCode, errorCount, timeSinceLastError});
                }
            } else {
                // Reset count if outside window
                errorCounts.put(errorCode, 1);
            }
        }
    }
    
    /**
     * Gets a formatted error report
     *
     * @return formatted error report
     */
    public String getErrorReport() {
        StringBuilder report = new StringBuilder();
        report.append("Error Report - ").append(System.currentTimeMillis()).append("\n");
        report.append("Total error types: ").append(errorCounts.size()).append("\n");
        
        errorCounts.forEach((errorCode, count) -> {
            Long lastErrorTime = lastErrorTimes.get(errorCode);
            report.append(String.format("  %s: %d errors, last: %s\n", 
                errorCode, count, 
                lastErrorTime != null ? new java.util.Date(lastErrorTime) : "N/A"));
        });
        
        return report.toString();
    }
} 