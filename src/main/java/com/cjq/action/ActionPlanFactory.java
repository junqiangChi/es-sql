package com.cjq.action;

import com.cjq.domain.Client;
import com.cjq.exception.ErrorCode;
import com.cjq.exception.EsSqlParseException;
import com.cjq.plan.logical.*;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public class ActionPlanFactory {
    private static volatile ActionPlanFactory instance;
    
    // Use mapping table to store creation strategies
    private final Map<Class<? extends LogicalPlan>, BiFunction<Client, LogicalPlan, ActionPlan>> actionPlanCreators;

    private ActionPlanFactory() {
        actionPlanCreators = new HashMap<>();
        initializeCreators();
    }

    public static ActionPlanFactory getInstance() {
        if (instance == null) {
            synchronized (ActionPlanFactory.class) {
                if (instance == null) {
                    instance = new ActionPlanFactory();
                }
            }
        }
        return instance;
    }

    private void initializeCreators() {
        // Special handling for Query type
        actionPlanCreators.put(Query.class, (client, plan) -> {
            Query query = (Query) plan;
            return query.isAgg() ? new AggQueryActionPlan(plan) : new DefaultQueryActionPlan(plan);
        });
        
        // Standard handling for other types
        actionPlanCreators.put(Delete.class, (client, plan) -> new DeleteActionPlan(plan));
        actionPlanCreators.put(Show.class, (client, plan) -> new ShowActionPlan(plan));
        actionPlanCreators.put(Drop.class, (client, plan) -> new DropActionPlan(plan));
        actionPlanCreators.put(Insert.class, (client, plan) -> new InsertActionPlan(client, plan));
        actionPlanCreators.put(Update.class, (client, plan) -> new UpdateActionPlan(plan));
        actionPlanCreators.put(UpdateByQuery.class, (client, plan) -> new UpdateByQueryActionPlan(plan));
    }

    public ActionPlan createAction(Client client, LogicalPlan plan) {
        BiFunction<Client, LogicalPlan, ActionPlan> creator = actionPlanCreators.get(plan.getClass());
        
        if (creator != null) {
            return creator.apply(client, plan);
        }
        
        throw new EsSqlParseException(ErrorCode.UNSUPPORTED_PLAN_TYPE + plan.getClass().getName());
    }

    // Enum to identify LogicalPlan types, providing better type safety
    private enum LogicalPlanType {
        QUERY, DELETE, SHOW, DROP, INSERT, UPDATE, UPDATE_BY_QUERY
    }
}
