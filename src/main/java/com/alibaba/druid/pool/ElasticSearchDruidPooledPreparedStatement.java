package com.alibaba.druid.pool;


import com.cjq.action.ActionPlan;
import com.cjq.action.ActionPlanFactory;
import com.cjq.domain.EqlParserDriver;
import com.cjq.exception.EsSqlParseException;
import com.cjq.executor.Executor;
import com.cjq.executor.ExecutorFactory;
import com.cjq.handler.HandlerFactory;
import com.cjq.handler.ResponseHandler;
import com.cjq.jdbc.HandlerResult;
import com.cjq.plan.logical.LogicalPlan;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ElasticSearchDruidPooledPreparedStatement extends DruidPooledPreparedStatement {

    private final ElasticSearchConnection connection;

    public ElasticSearchDruidPooledPreparedStatement(DruidPooledConnection conn, PreparedStatementHolder holder) throws SQLException {
        super(conn, holder);
        connection = (ElasticSearchConnection) conn.getConnection();
    }

    private HandlerResult getObjectResult(LogicalPlan plan) {
        try {
            ActionPlanFactory actionPlanFactory = ActionPlanFactory.getInstance();
            ActionPlan actionPlan = actionPlanFactory.createAction(connection.getClient(), plan);
            ActionRequest explain = actionPlan.explain();
            ExecutorFactory executorFactory = ExecutorFactory.getInstance();
            Executor executor = executorFactory.createActionPlanExecutor(plan, connection.getClient());
            ActionResponse response = executor.execute(explain);
            HandlerFactory handlerFactory = HandlerFactory.getInstance();
            ResponseHandler handler = handlerFactory.createHandler(plan, connection.getProperties());
            return handler.handle(response);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
            HandlerResult handlerResult = getObjectResult(plan);
            ResultSet rs = new ElasticSearchResultSet(this, handlerResult.getHeaders(), handlerResult.getRows());
            DruidPooledResultSet poolableResultSet = new DruidPooledResultSet(this, rs);
            addResultSetTrace(poolableResultSet);
            return poolableResultSet;
        } catch (Exception e) {
            throw new EsSqlParseException(e);
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
        return true;
    }

    @Override
    public int executeUpdate() throws SQLException {
        throw new SQLException("executeUpdate not support in ElasticSearch");
    }
}
