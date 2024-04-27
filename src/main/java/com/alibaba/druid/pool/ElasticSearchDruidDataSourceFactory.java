package com.alibaba.druid.pool;


import com.cjq.domain.Client;

import javax.sql.DataSource;
import java.util.Map;
import java.util.Properties;

/**
 * Created by allwefantasy on 8/30/16.
 */
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
