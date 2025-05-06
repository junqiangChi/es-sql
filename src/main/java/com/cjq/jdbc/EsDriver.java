package com.cjq.jdbc;

import com.cjq.exception.EsSqlParseException;
import com.cjq.exception.JdbcUrlException;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public class EsDriver implements Driver {
    private static final String CONNECT_STRING_PREFIX = "jdbc:elasticsearch:";

    private static final Driver INSTANCE = new EsDriver();
    private static boolean registered;

    static {
        load();
    }

    public static synchronized void load() {
        if (!registered) {
            registered = true;
            try {
                DriverManager.registerDriver(INSTANCE);
            } catch (SQLException throwable) {
                throw new JdbcUrlException(throwable);
            }
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (acceptsURL(url)) {
            return new EsConnection(url, info);
        }
        throw new EsSqlParseException("invalid url: " + url);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (url == null) {
            System.exit(1);
        }
        return url.startsWith(CONNECT_STRING_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }
}
