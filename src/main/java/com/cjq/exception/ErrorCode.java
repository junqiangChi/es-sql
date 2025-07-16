package com.cjq.exception;

/**
 * Error codes for ES SQL project
 * Provides centralized error code management with descriptive messages
 */
public enum ErrorCode {
    
    // SQL Parsing Errors (1000-1999)
    SQL_PARSE_ERROR("ES-1000", "SQL parsing failed"),
    SQL_SYNTAX_ERROR("ES-1001", "SQL syntax error"),
    UNSUPPORTED_SQL_TYPE("ES-1002", "Unsupported SQL type"),
    INVALID_FIELD_NAME("ES-1003", "Invalid field name"),
    MISSING_REQUIRED_CLAUSE("ES-1004", "Missing required SQL clause"),
    
    // Connection Errors (2000-2999)
    CONNECTION_FAILED("ES-2000", "Failed to connect to Elasticsearch"),
    CONNECTION_TIMEOUT("ES-2001", "Connection timeout"),
    AUTHENTICATION_FAILED("ES-2002", "Authentication failed"),
    INVALID_JDBC_URL("ES-2003", "Invalid JDBC URL format"),
    HOST_NOT_FOUND("ES-2004", "Elasticsearch host not found"),
    
    // Execution Errors (3000-3999)
    EXECUTION_FAILED("ES-3000", "Query execution failed"),
    INDEX_NOT_FOUND("ES-3001", "Index not found"),
    PERMISSION_DENIED("ES-3002", "Permission denied"),
    QUERY_TIMEOUT("ES-3003", "Query execution timeout"),
    INVALID_QUERY_TYPE("ES-3004", "Invalid query type for operation"),
    
    // Data Processing Errors (4000-4999)
    DATA_PARSING_ERROR("ES-4000", "Data parsing error"),
    INVALID_DATA_TYPE("ES-4001", "Invalid data type"),
    DATA_CONVERSION_ERROR("ES-4002", "Data conversion error"),
    MISSING_REQUIRED_FIELD("ES-4003", "Missing required field"),
    
    // Configuration Errors (5000-5999)
    CONFIGURATION_ERROR("ES-5000", "Configuration error"),
    MISSING_CONFIGURATION("ES-5001", "Missing required configuration"),
    INVALID_CONFIGURATION("ES-5002", "Invalid configuration value"),
    
    // Resource Errors (6000-6999)
    RESOURCE_NOT_FOUND("ES-6000", "Resource not found"),
    RESOURCE_ACCESS_DENIED("ES-6001", "Resource access denied"),
    RESOURCE_LIMIT_EXCEEDED("ES-6002", "Resource limit exceeded"),
    
    // System Errors (9000-9999)
    INTERNAL_ERROR("ES-9000", "Internal system error"),
    UNEXPECTED_ERROR("ES-9001", "Unexpected error occurred");

    private final String code;
    private final String defaultMessage;
    
    ErrorCode(String code, String defaultMessage) {
        this.code = code;
        this.defaultMessage = defaultMessage;
    }
    
    /**
     * Gets the error code
     *
     * @return the error code
     */
    public String getCode() {
        return code;
    }
    
    /**
     * Gets the default error message
     *
     * @return the default error message
     */
    public String getDefaultMessage() {
        return defaultMessage;
    }
    
    /**
     * Creates a formatted error message with the default message
     *
     * @return formatted error message
     */
    public String getFormattedMessage() {
        return String.format("[%s] %s", code, defaultMessage);
    }
    
    /**
     * Creates a formatted error message with custom message
     *
     * @param customMessage the custom message
     * @return formatted error message
     */
    public String getFormattedMessage(String customMessage) {
        return String.format("[%s] %s", code, customMessage);
    }
    
    @Override
    public String toString() {
        return getFormattedMessage();
    }
} 