package com.alibaba.druid.pool;


import com.cjq.domain.Client;
import com.cjq.domain.EqlParserDriver;
import com.cjq.jdbc.ObjectResult;
import com.cjq.jdbc.HandleResult;
import com.cjq.plan.logical.LogicalPlan;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by allwefantasy on 8/30/16.
 */
public class ElasticSearchDruidPooledPreparedStatement extends DruidPooledPreparedStatement {

    private final ElasticSearchConnection connection;
    private final Client client;
    private final Properties properties;

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

            ObjectResult objectResult = new HandleResult(client, plan, properties).getObjectResultSet();
            ResultSet rs = new ElasticSearchResultSet(this, objectResult.getHeaders(), objectResult.getRows());
            DruidPooledResultSet poolableResultSet = new DruidPooledResultSet(this, rs);
            addResultSetTrace(poolableResultSet);
            return poolableResultSet;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            connection.close();
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

    @Override
    public int executeUpdate() throws SQLException {
        throw new SQLException("executeUpdate not support in ElasticSearch");
    }
}
