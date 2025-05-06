package com.cjq;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.ElasticSearchDruidDataSourceFactory;
import com.cjq.common.ElasticsearchJdbcConfig;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class JdbcTest {
    Properties properties = new Properties();

    @Before
    public void before() {
        properties.put(ElasticsearchJdbcConfig.ES_URL.getName(), "jdbc:elasticsearch://localhost:9200");
        properties.put(ElasticsearchJdbcConfig.USERNAME.getName(), "elastic");
        properties.put(ElasticsearchJdbcConfig.PASSWORD.getName(), "123456");
        properties.put(ElasticsearchJdbcConfig.INCLUDE_INDEX.getName(), "true");
        properties.put(ElasticsearchJdbcConfig.INCLUDE_DOC_ID.getName(), "true");
    }

    @Test
    public void test() {
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
//            String sql = "SELECT * FROM my_index_1 t1 WHERE f3 NOT REGEXP '[a-z0-9]+' OR f1 > 1";
//            String sql = "SELECT * FROM my_index_1 t1 WHERE f3 REGEXP '[a-z0-9]+' AND f1 > 1";
//            String sql = "SELECT * FROM my_index_1 t1 WHERE f3 REGEXP '[a-z0-9]+' and f1 > 1";
//            String sql = "SELECT * FROM my_index_1 t1 WHERE f3 NOT REGEXP '[a-z0-9]+'";
//            String sql = "SELECT * FROM my_index_1 t1 WHERE f3 REGEXP '[a-z0-9]+'";
//            String sql = "SELECT * FROM my_index_1 t1 WHERE f1 between 2 and 3";
//            String sql = "SELECT * FROM my_index_1 t1 WHERE f1 not between 1 and 3";
//            String sql = "SELECT * FROM my_index_1 t1 WHERE f1 in (1,2,3)";
//            String sql = "SELECT * FROM my_index_1 t1 WHERE f1 not in (1,2,3)";
//            String sql = "SELECT * FROM my_index_1 WHERE f2 match '你好'";
//            String sql = "SELECT * FROM my_index_1 WHERE f2 term '你好'";
//            String sql = "SELECT * FROM my_index_1 WHERE f3 like 'cjq%'";
//            String sql = "SELECT * FROM my_index_1 WHERE f3 not like 'cjq%'";
        String sql = "select 1 f, '1' ff,f2, f1 from my_index order by f3";
        sqlExecute(sql);

    }

    private Connection getConnection() throws Exception {
        DruidDataSource dds = (DruidDataSource) ElasticSearchDruidDataSourceFactory.createDataSource(properties);
        return dds.getConnection();
    }

    private Connection getConnection2() throws Exception {
        Class.forName("com.cjq.jdbc.EsDriver");
        return DriverManager.getConnection(properties.getProperty("url"), properties);
    }

    private void sqlExecute(String sql) {
        try (Connection connection = getConnection2()) {
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
            System.out.println("查询条数：" + count);
            ps.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
