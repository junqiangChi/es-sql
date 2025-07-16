package com.cjq.common;

/**
 * Configuration properties for Elasticsearch JDBC connections
 * Defines all supported configuration options with their default values
 * Used for connection setup and query result formatting
 */
public enum ElasticsearchJdbcConfig {

    /** Username for Elasticsearch authentication */
    USERNAME("user", ""),
    
    /** Password for Elasticsearch authentication */
    PASSWORD("password", ""),
    
    /** Elasticsearch cluster URL */
    ES_URL("url", ""),
    
    /** Connection timeout in milliseconds */
    CONNECT_TIMEOUT("connect.timeout", "30000"),
    
    /** Socket timeout in milliseconds */
    SOCKET_TIMEOUT("socket.timeout", "60000"),
    
    /** Whether to include index name in query results */
    INCLUDE_INDEX("include.index.name", "false"),
    
    /** Whether to include document ID in query results */
    INCLUDE_DOC_ID("include.doc.id", "false"),
    
    /** Whether to include document type in query results */
    INCLUDE_TYPE("include.type", "false"),
    
    /** Whether to include search score in query results */
    INCLUDE_SCORE("include.score", "false");

    /** Configuration property name */
    private String name;
    
    /** Default value for the configuration property */
    private String defaultValue;

    /**
     * Constructs a configuration property with name and default value
     * 
     * @param name the property name
     * @param defaultValue the default value for the property
     */
    ElasticsearchJdbcConfig(String name, String defaultValue) {
        this.name = name;
        this.defaultValue = defaultValue;
    }

    /**
     * Gets the configuration property name
     * 
     * @return the property name
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the default value for this configuration property
     * 
     * @return the default value
     */
    public String getDefaultValue() {
        return defaultValue;
    }
}
