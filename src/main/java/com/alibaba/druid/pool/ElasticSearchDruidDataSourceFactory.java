package com.alibaba.druid.pool;


import javax.sql.DataSource;
import java.util.Properties;

public class ElasticSearchDruidDataSourceFactory extends DruidDataSourceFactory {

    @Override
    protected DataSource createDataSourceInternal(Properties properties) throws Exception {
        return new ElasticSearchDruidDataSource(properties);
    }

    public static DataSource createDataSource(Properties properties) throws Exception {
        return new ElasticSearchDruidDataSource(properties);
    }

    public static DataSource createDataSource() throws Exception {
        return new ElasticSearchDruidDataSource();
    }
}
