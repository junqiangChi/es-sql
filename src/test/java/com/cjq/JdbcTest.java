package com.cjq;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.druid.pool.ElasticSearchDruidDataSourceFactory;
import com.cjq.common.EsJdbcConfig;
import org.junit.Test;

import java.sql.*;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
            properties.put(EsJdbcConfig.INCLUDE_INDEX, "true");
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
                  "f1": 1,
                  "f2": "你好，世界",
                  "f3": "cjq945",
                  "f4": "cjq"
                }

                PUT my_index_1/_doc/2
                {
                  "f1": 2,
                  "f2": "你好，世界",
                  "f3": "cjq945",
                  "f4": "cjq"
                }
                PUT my_index_1/_doc/3
                {
                  "f1": 3,
                  "f2": "你好，世界",
                  "f3": "cjq945",
                  "f4": "cjq"
                }
                PUT my_index_1/_doc/4
                {
                  "f1":4,
                  "f2": "你好，世界"
                }

                PUT my_index_1/_doc/5
                {
                  "f1": 5,
                  "f2": "你好，世界"
                }
             */
            String sql = "SELECT * FROM my_index_1 t1 WHERE f1 between 2 and 3";
            //  String sql = "SELECT * FROM my_index_1 t1 WHERE f1 not between 1 and 3";
            //  String sql = "SELECT * FROM my_index_1 t1 WHERE f1 in (1,2,3)";
            //  String sql = "SELECT * FROM my_index_1 t1 WHERE f1 not in (1,2,3)";
            //  String sql = "SELECT * FROM my_index_1 t1 WHERE f1 > 3";
            //  String sql = "SELECT * FROM my_index_1 WHERE f2 match '你好'";
            //  String sql = "SELECT * FROM my_index_1 WHERE f2 term '你好'";
            //  String sql = "SELECT * FROM my_index_1 WHERE f3 not like 'cjq%'";
            //  String sql = "SELECT * FROM my_index_1 WHERE f4 = 'cjq'";
            /**
             * 以上测试全部正常
             */
            PreparedStatement ps = connection.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            Map<String, String> columnMap = new LinkedHashMap<>();
            for (int i = 1; i <= columnCount; i++) {
                columnMap.put(metaData.getColumnName(i), metaData.getColumnTypeName(i));
            }
            int count = 0;
            while (rs.next()) {
                HashMap<String, Object> resultMap = new LinkedHashMap<>();
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
