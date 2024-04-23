package com.alibaba.druid.pool;


import com.cjq.domain.Client;
import com.cjq.domain.EqlParserDriver;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by allwefantasy on 8/30/16.
 */
public class ElasticSearchDruidPooledPreparedStatement extends DruidPooledPreparedStatement {

  private final ElasticSearchConnection connection;
  private final Client client;

  public ElasticSearchDruidPooledPreparedStatement(DruidPooledConnection conn, PreparedStatementHolder holder) throws SQLException {
    super(conn, holder);
    connection = (ElasticSearchConnection) conn.getConnection();
    this.client =  connection.getClient();
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    checkOpen();

    incrementExecuteQueryCount();
    transactionRecord(getSql());

    oracleSetRowPrefetch();

    conn.beforeExecute();
    EqlParserDriver eqlParserDriver = connection.getEqlParserDriver();

   /* try {
      eqlParserDriver.parse(getSql());
      ObjectResult extractor = getObjectResult(true, false, true);
      List<String> headers = extractor.getHeaders();
      List<List<Object>> lines = extractor.getLines();

      ResultSet rs = new ElasticSearchResultSet(this, headers, lines);

      DruidPooledResultSet poolableResultSet = new DruidPooledResultSet(this, rs);
      addResultSetTrace(poolableResultSet);

      return poolableResultSet;
    } catch (Throwable t) {
      errorCheck(t);

      throw checkException(t);
    } finally {
      conn.afterExecute();
    }*/
    return null;
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
