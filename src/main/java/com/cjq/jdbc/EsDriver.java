package com.cjq.jdbc;

import com.cjq.exception.EsSqlParseException;
import com.cjq.exception.JdbcUrlException;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Elasticsearch JDBC Driver implementation
 * Provides JDBC connectivity to Elasticsearch clusters
 * Supports standard JDBC operations for querying Elasticsearch data
 */
public class EsDriver implements Driver {
    
    /** JDBC URL prefix for Elasticsearch connections */
    private static final String CONNECT_STRING_PREFIX = "jdbc:elasticsearch:";

    /** Singleton instance of the driver */
    private static final Driver INSTANCE = new EsDriver();
    
    /** Flag indicating whether the driver has been registered */
    private static boolean registered;

    /**
     * Static initializer block
     * Automatically loads and registers the driver when the class is loaded
     */
    static {
        load();
    }

    /**
     * Loads and registers the Elasticsearch JDBC driver
     * This method is synchronized to ensure thread-safe registration
     * 
     * @throws JdbcUrlException if driver registration fails
     */
    public static synchronized void load() {
        if (!registered) {
            registered = true;
            try {
                DriverManager.registerDriver(INSTANCE);
            } catch (SQLException throwable) {
                throw new JdbcUrlException("Failed to register Elasticsearch JDBC driver", throwable);
            }
        }
    }

    /**
     * Creates a new connection to the Elasticsearch cluster
     * 
     * @param url the JDBC URL for the Elasticsearch connection
     * @param info connection properties (username, password, etc.)
     * @return a new EsConnection instance
     * @throws SQLException if the URL is invalid or connection creation fails
     */
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (acceptsURL(url)) {
            return new EsConnection(url, info);
        }
        throw new EsSqlParseException("Invalid Elasticsearch JDBC URL: " + url);
    }

    /**
     * Checks if this driver can handle the given URL
     * 
     * @param url the JDBC URL to check
     * @return true if the URL starts with "jdbc:elasticsearch:"
     * @throws SQLException if the URL is null
     */
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (url == null) {
            throw new SQLException("URL cannot be null");
        }
        return url.startsWith(CONNECT_STRING_PREFIX);
    }

    /**
     * Gets driver property information
     * Currently returns an empty array as no specific properties are supported
     * 
     * @param url the JDBC URL
     * @param info connection properties
     * @return empty array of DriverPropertyInfo
     * @throws SQLException if an error occurs
     */
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    /**
     * Gets the major version number of the driver
     * 
     * @return major version number (currently 0)
     */
    @Override
    public int getMajorVersion() {
        return 0;
    }

    /**
     * Gets the minor version number of the driver
     * 
     * @return minor version number (currently 0)
     */
    @Override
    public int getMinorVersion() {
        return 0;
    }

    /**
     * Checks if this driver is JDBC compliant
     * 
     * @return false as this is a specialized driver for Elasticsearch
     */
    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    /**
     * Gets the parent logger for this driver
     * 
     * @return null as logging is not currently implemented
     * @throws SQLFeatureNotSupportedException if the feature is not supported
     */
    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }
}
