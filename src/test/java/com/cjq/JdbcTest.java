package com.cjq;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.ElasticSearchDruidDataSourceFactory;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 * @author: Julian Chi
 * @date: 2024/4/23 10:48
 * @description:
 */
public class JdbcTest {
    @Test
    public void test() {
        try {
            Properties properties = new Properties();
            properties.put("url", "jdbc:elasticsearch://192.168.2.163:9300");
            properties.setProperty("username", "elastic");
            properties.setProperty("password", "123456");


            String url = "jdbc:elasticsearch://192.168.10.100:9300";
            String usename = "elastic";
            String password = "123456";
            DruidDataSource dds = (DruidDataSource) ElasticSearchDruidDataSourceFactory.createDataSource(properties);

            Class.forName("");
            Connection connection = DriverManager.getConnection(url, usename, password);

            String sql = "SELECT * from ori_data_http limit 10";
            PreparedStatement ps = connection.prepareStatement(sql);
            ps.executeQuery();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
