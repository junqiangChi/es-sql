package com.cjq.domain;

import com.cjq.common.ElasticsearchJdbcConfig;
import com.cjq.exception.ErrorCode;
import com.cjq.exception.ExceptionUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Elasticsearch client wrapper for managing REST high-level client connections
 * Provides authentication, timeout configuration, and resource management
 */
public class Client implements AutoCloseable {
    
    // Constants for timeout configuration
    private static final int DEFAULT_CONNECT_TIMEOUT = 5000;
    private static final int DEFAULT_SOCKET_TIMEOUT = 60000;
    
    private final RestHighLevelClient restHighLevelClient;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    /**
     * Constructs a new Elasticsearch client with the specified hosts and properties
     *
     * @param httpHosts the Elasticsearch hosts to connect to
     * @param properties configuration properties for authentication and timeouts
     * @throws IllegalArgumentException if httpHosts is null or empty
     * @throws RuntimeException if client creation fails
     */
    public Client(HttpHost[] httpHosts, Properties properties) {
        validateHosts(httpHosts);
        validateProperties(properties);
        
        this.restHighLevelClient = createRestHighLevelClient(httpHosts, properties);
    }

    /**
     * Creates the REST high-level client with authentication and timeout configuration
     *
     * @param httpHosts the Elasticsearch hosts
     * @param properties configuration properties
     * @return configured RestHighLevelClient
     */
    private RestHighLevelClient createRestHighLevelClient(HttpHost[] httpHosts, Properties properties) {
        // Get authentication credentials
        String username = properties.getProperty(ElasticsearchJdbcConfig.USERNAME.getName());
        String password = properties.getProperty(ElasticsearchJdbcConfig.PASSWORD.getName());
        
        // Get timeout values
        int connectTimeout = getTimeoutValue(
            properties, 
            ElasticsearchJdbcConfig.CONNECT_TIMEOUT.getName(),
            ElasticsearchJdbcConfig.CONNECT_TIMEOUT.getDefaultValue()
        );
        
        int socketTimeout = getTimeoutValue(
            properties,
            ElasticsearchJdbcConfig.SOCKET_TIMEOUT.getName(),
            ElasticsearchJdbcConfig.SOCKET_TIMEOUT.getDefaultValue()
        );
        
        // Build the client
        if (username != null && password != null) {
            // With authentication
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(
                AuthScope.ANY, 
                new UsernamePasswordCredentials(username, password)
            );
            
            return new RestHighLevelClient(
                RestClient.builder(httpHosts)
                    .setHttpClientConfigCallback(
                        httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                    )
                    .setRequestConfigCallback(
                        requestConfigBuilder -> requestConfigBuilder
                            .setConnectTimeout(connectTimeout)
                            .setSocketTimeout(socketTimeout)
                    )
            );
        } else {
            // Without authentication
            return new RestHighLevelClient(
                RestClient.builder(httpHosts)
                    .setRequestConfigCallback(
                        requestConfigBuilder -> requestConfigBuilder
                            .setConnectTimeout(connectTimeout)
                            .setSocketTimeout(socketTimeout)
                    )
            );
        }
    }

    /**
     * Extracts and validates timeout values from properties
     *
     * @param properties configuration properties
     * @param propertyName the property name to extract
     * @param defaultValue the default value if property is not found
     * @return the timeout value in milliseconds
     */
    private int getTimeoutValue(Properties properties, String propertyName, String defaultValue) {
        return ExceptionUtils.executeWithExceptionHandling(() -> {
            String timeoutStr = properties.getProperty(propertyName, defaultValue);
            return Integer.parseInt(timeoutStr);
        }, ErrorCode.INVALID_CONFIGURATION, "Invalid timeout configuration: " + propertyName);
    }

    /**
     * Validates that the provided hosts array is not null or empty
     *
     * @param httpHosts the hosts to validate
     * @throws IllegalArgumentException if hosts are invalid
     */
    private void validateHosts(HttpHost[] httpHosts) {
        ExceptionUtils.validateNotNull(httpHosts, "HTTP hosts");
        ExceptionUtils.validateCondition(httpHosts.length > 0, "HTTP hosts cannot be empty");
    }

    /**
     * Validates that the provided properties are not null
     *
     * @param properties the properties to validate
     * @throws IllegalArgumentException if properties are null
     */
    private void validateProperties(Properties properties) {
        ExceptionUtils.validateNotNull(properties, "Properties");
    }

    /**
     * Gets the underlying REST high-level client
     *
     * @return the RestHighLevelClient instance
     * @throws IllegalStateException if the client is closed
     */
    public RestHighLevelClient getClient() {
        ExceptionUtils.validateCondition(!isClosed.get(), "Client is closed");
        return this.restHighLevelClient;
    }

    /**
     * Checks if the client is closed
     *
     * @return true if the client is closed, false otherwise
     */
    public boolean isClosed() {
        return isClosed.get();
    }

    /**
     * Closes the client and releases associated resources
     * This method is idempotent - calling it multiple times has no effect
     */
    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            try {
                if (restHighLevelClient != null) {
                    restHighLevelClient.close();
                }
            } catch (IOException e) {
                ExceptionUtils.executeWithExceptionHandling(() -> {
                    throw new RuntimeException("Failed to close Elasticsearch client", e);
                }, ErrorCode.INTERNAL_ERROR, "Failed to close Elasticsearch client");
            }
        }
    }

    /**
     * Ensures the client is closed when this object is garbage collected
     * This is a safety mechanism, but explicit close() calls are preferred
     */
    @Override
    protected void finalize() throws Throwable {
        try {
            close();
        } finally {
            super.finalize();
        }
    }
}
