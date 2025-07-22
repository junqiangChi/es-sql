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
    TYPE_CASE("ES-4004", "Type cast failed: "),

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
    UNEXPECTED_ERROR("ES-9001", "Unexpected error occurred"),

    // Error messages

    UNSUPPORTED_NESTED_FUNCTION("ES-1100", "Unsupported nested in function field: "),
    DUPLICATE_FIELD("ES-1101", "Duplicate field: "),
    UNSUPPORTED_PLAN_TYPE("ES-1102", "Unsupported logical plan type: "),
    UNKNOWN_WHERE_TYPE("ES-1103", "Unknown where type: "),
    UNKNOWN_FUNCTION_TYPE("ES-1104", "Unknown function type: "),
    CLIENT_NOT_INITIALIZED("ES-1105", "Client is not initialized"),
    NODECLIENT_NOT_INITIALIZED("ES-1106", "NodeClient is not initialized"),
    RESPONSE_NULL("ES-1107", "Response cannot be null"),
    LOGICAL_PLAN_NULL("ES-1108", "LogicalPlan cannot be null"),
    NOT_AGGREGATION_FIELD("ES-1109", "Not aggregation field: "),
    ONLY_AGGREGATION_FIELD_WITHOUT_GROUP_BY("ES-1110", "Only aggregation field without group by"),
    SORT_FIELD_ONLY_SUPPORT_NAME_OR_ALIAS("ES-1111", "Sort field only support name or alias"),
    UNSUPPORTED_FUNCTION("ES-1112", "Unsupported function: "),
    UNSUPPORTED_AGGREGATION_FUNCTION("ES-1113", "Unsupported aggregation function: ");


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