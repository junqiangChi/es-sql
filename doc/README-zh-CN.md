# Es-Sql

## 介绍

Es-Sql是使用SQL语法操作[`Elasticsearch`](https://github.com/elastic/elasticsearch)
的项目，其中语法解析使用了[`Antlr4`](https://github.com/antlr/antlr4)
组件，数据库连接池使用[`Druid`](https://github.com/alibaba/druid)。

## 构建

```shell
git clone https://github.com/junqiangChi/es-sql.git
mvn clean install es-sql -SkipTests=true
```

## EsJdbcConfig

使用Jdbc连接时可设置的配置参数

| 参数key              | 默认值    | 类型      | 描述                                               |
|--------------------|--------|---------|--------------------------------------------------|
| user               | (none) | String  | Elasticsearch用户名                                 |
| password           | (none) | String  | Elasticsearch用户密码                                |
| url                | (none) | String  | jdbc连接的Url，例：jdbc:elasticsearch://localhost:9200 |
| include.index.name | false  | Boolean | 查询结果是否包含索引名                                      |
| include.doc.id     | false  | Boolean | 查询结果是否包含doc_id                                   |
| include.type       | false  | Boolean | 查询结果是否包含类型                                       |
| include.score      | false  | Boolean | 查询结果是否包含score                                    |

## SQL语法支持特性

- ✅ SELECT
    - ✅ 字段别名
    - ✅ 常量字段
- ✅ WHERE
    - ✅ =
    - ✅ >
    - ✅ <
    - ✅ >=
    - ✅ <=
    - ✅ !=
    - ✅ IS
    - ✅ IS NOT
    - ✅ LIKE
    - ✅ NOT LIKE
    - ✅ IN
    - ✅ NOT IN
    - ✅ BETWEEN
    - ✅ NBETWEEN
    - ✅ REGEXP
    - ✅ NREGEXP
    - ✅ MATCH
    - ✅ MATCH_PHRASE
    - ✅ TERM
- ✅ ORDER BY
- ✅ GROUP BY
    - ✅ FUNCTION
        - ✅ COUNT()
        - ✅ MAX()
        - ✅ MIN()
        - ✅ SUM()
        - ✅ AVG()
- ✅ LIMIT
    - ✅ LIMIT 1
    - ✅ LIMIT 1, 5
- ✅ DROP
    - ✅ DROP TABLE [IF EXITS] TABLE_NAME
- ✅ DELETE
    - ✅ DELETE FROM TABLE_NAME [WHERE]

## 创建jdbc连接

### Driver和Druid两种创建连接方式

```java
import java.sql.Connection;
import java.util.Properties;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.ElasticSearchDruidDataSourceFactory;

public class JdbcTest {

    @Test
    public void createConnectionWithDriver() {
        Class.forName("com.cjq.jdbc.EsDriver");
        String url = "jdbc:elasticsearch://localhost:9200";
        Properties properties = new Properties();
        properties.put("user", "");
        properties.put("password", "");
        Connection connection = DriverManager.getConnection(url, properties);
    }

    @Test
    public void createConnectionWithDruid() {
        Properties properties = new Properties();
        properties.put("url", "jdbc:elasticsearch://localhost:9200");
        properties.put("user", "");
        properties.put("password", "");
        DruidDataSource dds = (DruidDataSource) ElasticSearchDruidDataSourceFactory.createDataSource(properties);
        Connection connection = dds.getConnection();
    }
}
```
