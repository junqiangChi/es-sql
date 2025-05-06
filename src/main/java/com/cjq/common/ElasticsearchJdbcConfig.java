package com.cjq.common;

public enum ElasticsearchJdbcConfig {

    USERNAME("user", ""),
    PASSWORD("password", ""),
    ES_URL("url", ""),
    CONNECT_TIMEOUT("connect.timeout", "30000"),
    SOCKET_TIMEOUT("socket.timeout", "60000"),
    INCLUDE_INDEX("include.index.name", "false"),
    INCLUDE_DOC_ID("include.doc.id", "false"),
    INCLUDE_TYPE("include.type", "false"),
    INCLUDE_SCORE("include.score", "false");

    private String name;
    private String defaultValue;

    ElasticsearchJdbcConfig(String name, String defaultValue) {
        this.name = name;
        this.defaultValue = defaultValue;
    }

    public String getName() {
        return name;
    }

    public String getDefaultValue() {
        return defaultValue;
    }
}
