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
//        String sql = "select sum(f1) g1, avg(f2) g2 from my_index1 order by g2 desc, f3 desc ";
//        String sql = "select concat_ws('_',upper(f1),lower(f2)) ff from my_index";
//        String sql = "select sum(f5.f51) from my_index";
//        String sql = "select case when f5.f51 = '111' then '111' else '222' end f from my_index";
//        String sql = "select f3,f4,sum(f1) as f33, avg(f2) as avg from my_index1 group by f3,f4";
//        String sql = "drop table my_index11";
//        String sql = "show tables";
//        String sql = "show tables like 'my_*'";
//        String sql = "select substr(f3, 1, 1) f ,f3 from my_index";
//        String sql = "select abs(f1) f ,f3 from my_index";
//        String sql = "select ROUND(100.5 ) f ,f3 from my_index";
//        String sql = "select sqrt(100) f ,f3 from my_index";
//        String sql = "select pow(f3, 2) f,f1,f2,f3,f4,f5 from my_index";
//        String sql = "select replace(f3, '1', '0') f ,f3 from my_index";
        String sql = "select ifNUll(null, f3) f ,f3 from my_index";
        sqlExecuteQuery(sql);
//        String sql = "drop table my_index123";
//        String sql = "drop table if exists my_index123";
//        String sql = "update my_index1 set f2 = 2312 by '8'";
//        String sql = "update my_index1 set f2 = 2312 where f1 = 1231231";
//        sqlExecute(sql);

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
            boolean execute = ps.execute();
            System.out.println("是否执行成功: " + execute);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void sqlExecuteQuery(String sql) {
        System.out.println(sql);
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
