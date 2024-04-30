package com.cjq;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.druid.pool.ElasticSearchDruidDataSourceFactory;
import com.cjq.common.EsJdbcConfig;
import org.junit.Test;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
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
            properties.put(EsJdbcConfig.ES_URL, "jdbc:elasticsearch://node001:9200");
            properties.put(EsJdbcConfig.USERNAME, "elastic");
            properties.put(EsJdbcConfig.PASSWORD, "123456");
            DruidDataSource dds = (DruidDataSource) ElasticSearchDruidDataSourceFactory.createDataSource(properties);

            DruidPooledConnection connection = dds.getConnection();
            /*
                PUT my_index_1/
                {
                  "mappings": {
                    "properties": {
                      "f1": {
                        "type": "long"
                      },
                      "f2": {
                        "type": "text",
                        "analyzer": "ik_smart"
                      },
                      "f3": {
                        "type": "keyword"
                      },
                      "f4": {
                        "type": "text"
                      }
                    }
                  }
                }

                PUT my_index_1/_doc/1
                {
                  "f1": 5,
                  "f2": "你好，世界",
                  "f3": "cjq945",
                  "f4": "cjq"
                }
             */
            String sql = "SELECT * FROM my_index_1 WHERE f1 > 2";
            // String sql = "SELECT * FROM my_index_1 WHERE f2 match '你好'";
            // String sql = "SELECT * FROM my_index_1 WHERE f3 like 'cjq%'";
            // String sql = "SELECT * FROM my_index_1 WHERE f4 = 'cjq'";
            /**
             * 以上测试全部正常
             */
            PreparedStatement ps = connection.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            Map<String, String> columnMap = new HashMap<>();
            for (int i = 1; i <= columnCount; i++) {
                columnMap.put(metaData.getColumnName(i), metaData.getColumnTypeName(i));
            }
            int count = 0;
            while (rs.next()) {
                HashMap<String, Object> resultMap = new HashMap<>();
                columnMap.forEach((key, value) -> {
                    try {
                        resultMap.put(key, rs.getObject(key));
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                });
                System.out.println(resultMap);
                count++;
            }
            System.out.println(count);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
