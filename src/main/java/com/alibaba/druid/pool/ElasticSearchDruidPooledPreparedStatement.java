package com.alibaba.druid.pool;


import com.cjq.common.EsJdbcConfig;
import com.cjq.domain.Client;
import com.cjq.domain.EqlParserDriver;
import com.cjq.domain.HandleRequest;
import com.cjq.jdbc.ObjectResult;
import com.cjq.plan.logical.LogicalPlan;
import com.cjq.plan.logical.Query;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by allwefantasy on 8/30/16.
 */
public class ElasticSearchDruidPooledPreparedStatement extends DruidPooledPreparedStatement {

    private final ElasticSearchConnection connection;
    private final Client client;
    private Properties properties;

    public ElasticSearchDruidPooledPreparedStatement(DruidPooledConnection conn, PreparedStatementHolder holder) throws SQLException {
        super(conn, holder);
        connection = (ElasticSearchConnection) conn.getConnection();
        this.client = connection.getClient();
        this.properties = connection.getProperties();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        checkOpen();

        incrementExecuteQueryCount();
        transactionRecord(getSql());

        oracleSetRowPrefetch();

        try {
            conn.beforeExecute();
            EqlParserDriver eqlParserDriver = connection.getEqlParserDriver();
            LogicalPlan plan = eqlParserDriver.parser(getSql());

            if (plan instanceof Query) {
                Query query = (Query) plan;
                RestHighLevelClient restHighLevelClient = client.getClient();
                ObjectResult objectResult = getObjectResult(query, restHighLevelClient);

                ResultSet rs = new ElasticSearchResultSet(this, objectResult.getHeaders(), objectResult.getRows());

                DruidPooledResultSet poolableResultSet = new DruidPooledResultSet(this, rs);
                addResultSetTrace(poolableResultSet);
                return poolableResultSet;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            connection.close();
        }
        return null;
    }

    private ObjectResult getObjectResult(Query query, RestHighLevelClient restHighLevelClient) throws IOException {
        HandleRequest handleRequest = new HandleRequest();
        SearchResponse searchResponse = handleRequest.search(query, restHighLevelClient);
        SearchHit[] hits = searchResponse.getHits().getHits();
        List<String> headers = new ArrayList<>();
        List<List<Object>> rows = new ArrayList<>();
        setResult(hits, headers, rows);
        return new ObjectResult(headers, rows);
    }

    private void setResult(SearchHit[] hits, List<String> headers, List<List<Object>> rows) {
        for (SearchHit hit : hits) {
            Map<String, Object> doc = hit.getSourceAsMap();
            headers.addAll(doc.keySet());
            rows.add(new ArrayList<>(doc.values()));
        }
    }

    @Override
    public boolean execute() throws SQLException {
        checkOpen();

        incrementExecuteCount();
        transactionRecord(getSql());

        // oracleSetRowPrefetch();

        conn.beforeExecute();
    /*try {
      ObjectResult extractor = getObjectResult(true, false, true);
      List<String> headers = extractor.getHeaders();
      List<List<Object>> lines = extractor.getLines();

      ResultSet rs = new ElasticSearchResultSet(this, headers, lines);
      ((ElasticSearchPreparedStatement) getRawPreparedStatement()).setResults(rs);

      return true;
    } catch (Throwable t) {
      errorCheck(t);

      throw checkException(t);
    } finally {
      conn.afterExecute();
    }*/
        return true;
    }

 /* private ObjectResult getObjectResult(boolean flat, boolean includeScore, boolean includeId) throws SqlParseException, SQLFeatureNotSupportedException, Exception, CsvExtractorException {
    SearchDao searchDao = new org.nlpcn.es4sql.SearchDao(client);

    String query = ((ElasticSearchPreparedStatement) getRawPreparedStatement()).getExecutableSql();
    QueryAction queryAction = searchDao.explain(query);
    Object execution = QueryActionElasticExecutor.executeAnyAction(searchDao.getClient(), queryAction);
    return new ObjectResultsExtractor(includeScore, includeId, false, queryAction).extractResults(execution, flat);
  }*/

    @Override
    public int executeUpdate() throws SQLException {
        throw new SQLException("executeUpdate not support in ElasticSearch");
    }
}
