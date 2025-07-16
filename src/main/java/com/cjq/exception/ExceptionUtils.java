package com.cjq.exception;

import java.util.function.Supplier;

/**
 * Utility class for exception handling
 * Provides convenient methods for exception creation and handling
 */
public final class ExceptionUtils {
    
    private ExceptionUtils() {
        // Utility class, prevent instantiation
    }
    
    /**
     * Creates a SQL parse exception with default error code
     *
     * @param message the error message
     * @return EsSqlParseException
     */
    public static EsSqlParseException createSqlParseException(String message) {
        return new EsSqlParseException(message);
    }
    
    /**
     * Creates a SQL parse exception with custom error code
     *
     * @param errorCode the error code
     * @param message the error message
     * @return EsSqlParseException
     */
    public static EsSqlParseException createSqlParseException(ErrorCode errorCode, String message) {
        return new EsSqlParseException(errorCode, message);
    }
    
    /**
     * Creates a SQL parse exception with context
     *
     * @param message the error message
     * @param context the error context
     * @return EsSqlParseException
     */
    public static EsSqlParseException createSqlParseException(String message, String context) {
        return new EsSqlParseException(message, context);
    }
    
    /**
     * Creates a JDBC URL exception with default error code
     *
     * @param message the error message
     * @return JdbcUrlException
     */
    public static JdbcUrlException createJdbcUrlException(String message) {
        return new JdbcUrlException(message);
    }
    
    /**
     * Creates a JDBC URL exception with custom error code
     *
     * @param errorCode the error code
     * @param message the error message
     * @return JdbcUrlException
     */
    public static JdbcUrlException createJdbcUrlException(ErrorCode errorCode, String message) {
        return new JdbcUrlException(errorCode, message);
    }
    
    /**
     * Creates an Elasticsearch execute exception with default error code
     *
     * @param message the error message
     * @return ElasticsearchExecuteException
     */
    public static ElasticsearchExecuteException createExecuteException(String message) {
        return new ElasticsearchExecuteException(message);
    }
    
    /**
     * Creates an Elasticsearch execute exception with custom error code
     *
     * @param errorCode the error code
     * @param message the error message
     * @return ElasticsearchExecuteException
     */
    public static ElasticsearchExecuteException createExecuteException(ErrorCode errorCode, String message) {
        return new ElasticsearchExecuteException(errorCode, message);
    }
    
    /**
     * Creates an Elasticsearch execute exception with context
     *
     * @param message the error message
     * @param context the error context
     * @return ElasticsearchExecuteException
     */
    public static ElasticsearchExecuteException createExecuteException(String message, String context) {
        return new ElasticsearchExecuteException(message, context);
    }
    
    /**
     * Throws a SQL parse exception if the condition is true
     *
     * @param condition the condition to check
     * @param message the error message
     * @throws EsSqlParseException if condition is true
     */
    public static void throwIfSqlParseError(boolean condition, String message) {
        if (condition) {
            throw createSqlParseException(message);
        }
    }
    
    /**
     * Throws a SQL parse exception if the condition is true
     *
     * @param condition the condition to check
     * @param errorCode the error code
     * @param message the error message
     * @throws EsSqlParseException if condition is true
     */
    public static void throwIfSqlParseError(boolean condition, ErrorCode errorCode, String message) {
        if (condition) {
            throw createSqlParseException(errorCode, message);
        }
    }
    
    /**
     * Throws a JDBC URL exception if the condition is true
     *
     * @param condition the condition to check
     * @param message the error message
     * @throws JdbcUrlException if condition is true
     */
    public static void throwIfJdbcUrlError(boolean condition, String message) {
        if (condition) {
            throw createJdbcUrlException(message);
        }
    }
    
    /**
     * Throws an Elasticsearch execute exception if the condition is true
     *
     * @param condition the condition to check
     * @param message the error message
     * @throws ElasticsearchExecuteException if condition is true
     */
    public static void throwIfExecuteError(boolean condition, String message) {
        if (condition) {
            throw createExecuteException(message);
        }
    }
    
    /**
     * Throws an Elasticsearch execute exception if the condition is true
     *
     * @param condition the condition to check
     * @param errorCode the error code
     * @param message the error message
     * @throws ElasticsearchExecuteException if condition is true
     */
    public static void throwIfExecuteError(boolean condition, ErrorCode errorCode, String message) {
        if (condition) {
            throw createExecuteException(errorCode, message);
        }
    }
    
    /**
     * Executes a supplier and handles exceptions with the exception handler
     *
     * @param supplier the supplier to execute
     * @param errorCode the error code to use if an exception occurs
     * @param <T> the return type
     * @return the result of the supplier
     * @throws BaseException if an exception occurs
     */
    public static <T> T executeWithExceptionHandling(Supplier<T> supplier, ErrorCode errorCode) {
        try {
            return supplier.get();
        } catch (Exception e) {
            ExceptionHandler.getInstance().handleException(e);
            throw ExceptionHandler.getInstance().createBaseException(e, errorCode);
        }
    }
    
    /**
     * Executes a supplier and handles exceptions with the exception handler
     *
     * @param supplier the supplier to execute
     * @param errorCode the error code to use if an exception occurs
     * @param context the error context
     * @param <T> the return type
     * @return the result of the supplier
     * @throws BaseException if an exception occurs
     */
    public static <T> T executeWithExceptionHandling(Supplier<T> supplier, ErrorCode errorCode, String context) {
        try {
            return supplier.get();
        } catch (Exception e) {
            ExceptionHandler.getInstance().handleException(e);
            throw ExceptionHandler.getInstance().createBaseException(e, errorCode, context);
        }
    }
    
    /**
     * Executes a runnable and handles exceptions with the exception handler
     *
     * @param runnable the runnable to execute
     * @param errorCode the error code to use if an exception occurs
     * @throws BaseException if an exception occurs
     */
    public static void executeWithExceptionHandling(Runnable runnable, ErrorCode errorCode) {
        try {
            runnable.run();
        } catch (Exception e) {
            ExceptionHandler.getInstance().handleException(e);
            throw ExceptionHandler.getInstance().createBaseException(e, errorCode);
        }
    }
    
    /**
     * Executes a runnable and handles exceptions with the exception handler
     *
     * @param runnable the runnable to execute
     * @param errorCode the error code to use if an exception occurs
     * @param context the error context
     * @throws BaseException if an exception occurs
     */
    public static void executeWithExceptionHandling(Runnable runnable, ErrorCode errorCode, String context) {
        try {
            runnable.run();
        } catch (Exception e) {
            ExceptionHandler.getInstance().handleException(e);
            throw ExceptionHandler.getInstance().createBaseException(e, errorCode, context);
        }
    }
    
    /**
     * Validates that an object is not null
     *
     * @param object the object to validate
     * @param name the name of the object for error message
     * @throws IllegalArgumentException if object is null
     */
    public static void validateNotNull(Object object, String name) {
        if (object == null) {
            throw new IllegalArgumentException(name + " cannot be null");
        }
    }
    
    /**
     * Validates that a string is not null or empty
     *
     * @param string the string to validate
     * @param name the name of the string for error message
     * @throws IllegalArgumentException if string is null or empty
     */
    public static void validateNotEmpty(String string, String name) {
        validateNotNull(string, name);
        if (string.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " cannot be empty");
        }
    }
    
    /**
     * Validates that a condition is true
     *
     * @param condition the condition to validate
     * @param message the error message if condition is false
     * @throws IllegalArgumentException if condition is false
     */
    public static void validateCondition(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
} 