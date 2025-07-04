package com.cjq.executor;

import com.cjq.domain.Client;
import com.cjq.exception.EsSqlParseException;
import com.cjq.plan.logical.*;

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
        }
        throw new EsSqlParseException("Unsupported plan type: " + plan.getClass().getName());
    }
}
