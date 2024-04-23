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

    public static class UpperCaseCharStream implements CharStream {

        private CodePointCharStream wrapped;

        public UpperCaseCharStream(CodePointCharStream wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public String getText(Interval interval) {
            return wrapped.getText(interval);
        }

        @Override
        public void consume() {
            wrapped.consume();
        }

        @Override
        public int LA(int i) {
            int la = wrapped.LA(i);
            if (la == 0 || la == IntStream.EOF) {
                return la;
            }
            return Character.toUpperCase(la);
        }

        @Override
        public int mark() {
            return wrapped.mark();
        }

        @Override
        public void release(int marker) {
            wrapped.release(marker);
        }

        @Override
        public int index() {
            return wrapped.index();
        }

        @Override
        public void seek(int index) {

        }

        @Override
        public int size() {
            return wrapped.size();
        }

        @Override
        public String getSourceName() {
            return wrapped.getSourceName();
        }
    }
}
