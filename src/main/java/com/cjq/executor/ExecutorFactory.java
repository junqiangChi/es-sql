package com.cjq.executor;

import com.cjq.domain.Client;
import com.cjq.exception.ErrorCode;
import com.cjq.exception.EsSqlParseException;
import com.cjq.plan.logical.*;
import org.elasticsearch.client.node.NodeClient;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public class ExecutorFactory {
    private static volatile ExecutorFactory instance;
    
    // Use mapping table to store creation strategies
    private final Map<Class<? extends LogicalPlan>, BiFunction<Client, LogicalPlan, Executor>> clientExecutorCreators;
    private final Map<Class<? extends LogicalPlan>, BiFunction<NodeClient, LogicalPlan, Executor>> nodeClientExecutorCreators;

    private ExecutorFactory() {
        clientExecutorCreators = new HashMap<>();
        nodeClientExecutorCreators = new HashMap<>();
        initializeCreators();
    }

    public static ExecutorFactory getInstance() {
        if (instance == null) {
            synchronized (ExecutorFactory.class) {
                if (instance == null) {
                    instance = new ExecutorFactory();
                }
            }
        }
        return instance;
    }

    private void initializeCreators() {
        // Initialize creators for Client type
        clientExecutorCreators.put(Query.class, (client, plan) -> new QueryExecutor(client));
        clientExecutorCreators.put(Delete.class, (client, plan) -> new DeleteExecutor(client));
        clientExecutorCreators.put(Show.class, (client, plan) -> new ShowExecutor(client));
        clientExecutorCreators.put(Drop.class, (client, plan) -> new DropExecutor(client));
        clientExecutorCreators.put(Insert.class, (client, plan) -> new InsertExecutor(client));
        clientExecutorCreators.put(Update.class, (client, plan) -> new UpdateExecutor(client));
        clientExecutorCreators.put(UpdateByQuery.class, (client, plan) -> new UpdateByQueryExecutor(client));

        // Initialize creators for NodeClient type
        nodeClientExecutorCreators.put(Query.class, (nodeClient, plan) -> new QueryExecutor(nodeClient));
        nodeClientExecutorCreators.put(Delete.class, (nodeClient, plan) -> new DeleteExecutor(nodeClient));
        nodeClientExecutorCreators.put(Show.class, (nodeClient, plan) -> new ShowExecutor(nodeClient));
        nodeClientExecutorCreators.put(Drop.class, (nodeClient, plan) -> new DropExecutor(nodeClient));
        nodeClientExecutorCreators.put(Insert.class, (nodeClient, plan) -> new InsertExecutor(nodeClient));
        nodeClientExecutorCreators.put(Update.class, (nodeClient, plan) -> new UpdateExecutor(nodeClient));
        nodeClientExecutorCreators.put(UpdateByQuery.class, (nodeClient, plan) -> new UpdateByQueryExecutor(nodeClient));
    }

    public Executor createActionPlanExecutor(LogicalPlan plan, Client client) {
        BiFunction<Client, LogicalPlan, Executor> creator = clientExecutorCreators.get(plan.getClass());
        
        if (creator != null) {
            return creator.apply(client, plan);
        }

        throw new EsSqlParseException(ErrorCode.UNSUPPORTED_PLAN_TYPE + plan.getClass().getName());
    }

    public Executor createActionPlanWebExecutor(LogicalPlan plan, NodeClient nodeClient) {
        BiFunction<NodeClient, LogicalPlan, Executor> creator = nodeClientExecutorCreators.get(plan.getClass());
        
        if (creator != null) {
            return creator.apply(nodeClient, plan);
        }
        
        throw new EsSqlParseException(ErrorCode.UNSUPPORTED_PLAN_TYPE + plan.getClass().getName());
    }
}
