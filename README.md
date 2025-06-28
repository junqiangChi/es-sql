# Es Sql

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Elastic Elasticsearch](./doc/images/Elastic-Elasticsearch.svg)](https://github.com/elastic/elasticsearch)
[![Antlr Antlr4](./doc/images/Antrl-Antrl4.svg)](https://github.com/antlr/antlr4)
[![Alibaba Druid](./doc/images/Alibaba-Druid.svg)](https://github.com/alibaba/druid)
[![Alibaba Druid](./doc/images/Java-1.8.svg)]()
[![CN doc](./doc/images/文档-中文版-blue.svg)](./doc/README-zh-CN.md)

## Introduction

Es SQL uses SQL syntax to operate on [`Elasticsearch`](https://github.com/elastic/elasticsearch)
The project used [`Antlr4`](https://github.com/antlr/antlr4) for syntax parsing
Component, database connection pool usage [`Druid`](https://github.com/alibaba/druid).

## Builds

```shell
git clone https://github.com/junqiangChi/es-sql.git
mvn clean package install -DskipTests
```

## Elasticsearch Plugin

```shell
./bin/elasticsearch-plugin install file:///elasticsearch-sql-plugin.zip
```

### Plugin Usage

```
GET _es_sql
{
  "sql": "select * from myindex"
}
GET _es_sql/explain
{
  "sql": "select * from myindex"
}
POST _es_sql/explain
{
  "sql": "select * from myindex"
}
POST _es_sql
{
  "sql": "select * from myindex"
}
```

## EsJdbcConfig

Configuration parameters that can be set when using Jdbc connection

| key                | default | type    | description                                |
|--------------------|---------|---------|--------------------------------------------|
| user               | (none)  | String  | Elasticsearch username                     |
| password           | (none)  | String  | Elasticsearch password                     |
| url                | (none)  | String  | Url eg：jdbc:elasticsearch://localhost:9200 |
| include.index.name | false   | Boolean | The query result include the index name    |
| include.doc.id     | false   | Boolean | The query result include the docId         |
| include.type       | false   | Boolean | The query result include the type          |
| include.score      | false   | Boolean | The query result include the score         |

## SQL Supported Features

- ✅ SELECT
    - ✅ Field Alias
    - ✅ Constant Field
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
- ✅ DELETE
    - ✅ DELETE FROM TABLE_NAME [WHERE]

## Create jdbc connection

### Use `Driver` or `Druid`  to create connections

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
