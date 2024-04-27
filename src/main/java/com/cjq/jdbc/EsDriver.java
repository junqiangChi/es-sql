package com.cjq.jdbc;

import com.alibaba.druid.pool.DruidDataSource;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.IntStream;
import org.antlr.v4.runtime.misc.Interval;

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

    private DruidDataSource dds;

    public static synchronized void load() {
        if (!registered) {
            registered = true;
            try {
                DriverManager.registerDriver(INSTANCE);
            } catch (SQLException throwable) {
                throwable.printStackTrace();
            }
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        String[] parts = url.split(":", 3);

        return null;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
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
