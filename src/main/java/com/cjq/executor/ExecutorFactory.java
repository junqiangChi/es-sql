package com.cjq.executor;

import com.cjq.domain.Client;
import com.cjq.exception.EsSqlParseException;
import com.cjq.plan.logical.*;
import org.elasticsearch.client.node.NodeClient;

public class ExecutorFactory {
    private static ExecutorFactory executorFactory;

    private ExecutorFactory() {
    }

    public static ExecutorFactory getInstance() {
        if (executorFactory == null) {
            executorFactory = new ExecutorFactory();
        }
        return executorFactory;
    }

    public Executor createActionPlanExecutor(LogicalPlan plan, Client client) {
        if (plan instanceof Query) {
            return new QueryExecutor(client);
        } else if (plan instanceof Delete) {
            return new DeleteExecutor(client);
        } else if (plan instanceof Show) {
            return new ShowExecutor(client);
        } else if (plan instanceof Drop) {
            return new DropExecutor(client);
        } else if (plan instanceof Insert) {
            return new InsertExecutor(client);
        } else if (plan instanceof Update) {
            return new UpdateExecutor(client);
        } else if (plan instanceof UpdateByQuery) {
            return new UpdateByQueryExecutor(client);
        }
        throw new EsSqlParseException("Unsupported plan type: " + plan.getClass().getName());
    }

    public Executor createActionPlanWebExecutor(LogicalPlan plan, NodeClient nodeClient) {
        if (plan instanceof Query) {
            return new QueryExecutor(nodeClient);
        } else if (plan instanceof Delete) {
            return new DeleteExecutor(nodeClient);
        } else if (plan instanceof Show) {
            return new ShowExecutor(nodeClient);
        } else if (plan instanceof Drop) {
            return new DropExecutor(nodeClient);
        } else if (plan instanceof Insert) {
            return new InsertExecutor(nodeClient);
        } else if (plan instanceof Update) {
            return new UpdateExecutor(nodeClient);
        } else if (plan instanceof UpdateByQuery) {
            return new UpdateByQueryExecutor(nodeClient);
        }
        throw new EsSqlParseException("Unsupported plan type: " + plan.getClass().getName());
    }
}
