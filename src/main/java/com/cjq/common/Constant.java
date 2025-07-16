package com.cjq.common;

/**
 * Constants used throughout the ES SQL project
 * Defines common field names and prefixes used in Elasticsearch operations
 */
public class Constant {
    
    /** Elasticsearch's metadata field name for index */
    public static final String _INDEX = "_index";
    
    /** Elasticsearch's metadata field name for document type */
    public static final String _TYPE = "_type";
    
    /** Elasticsearch's metadata field name for document ID */
    public static final String _ID = "_id";
    
    /** Elasticsearch's metadata field name for search score */
    public static final String _SCORE = "_score";
    
    /** Prefix used for group by field names in aggregate queries */
    public static final String GROUP_BY_PREFIX = "group_by_";

    public static final String WILDCARD_ALL = "*";
    public static final String PERCENT_SYMBOL = "%";
    public static final String WILDCARD_SYMBOL = "*";
    public static final int DEFAULT_FROM = 0;
    public static final int DEFAULT_SIZE = 1000;

    // Index related constants
    public static final String[] DEFAULT_INDICES = {"*"};
    public static final String HIDDEN_INDEX_PREFIX = ".";
    public static final String INDEX_COLUMN = "index";

    // Error messages
    public static final String ERROR_DUPLICATE_FIELD = "Duplicate field: ";
    public static final String ERROR_UNSUPPORTED_PLAN_TYPE = "Unsupported logical plan type: ";
    public static final String ERROR_UNKNOWN_WHERE_TYPE = "Unknown where type: ";
    public static final String ERROR_CLIENT_NOT_INITIALIZED = "Client is not initialized";
    public static final String ERROR_NODECLIENT_NOT_INITIALIZED = "NodeClient is not initialized";
    public static final String ERROR_RESPONSE_NULL = "Response cannot be null";
    public static final String ERROR_LOGICAL_PLAN_NULL = "LogicalPlan cannot be null";
}
