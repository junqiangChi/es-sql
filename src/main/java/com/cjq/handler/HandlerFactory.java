package com.cjq.handler;

import com.cjq.exception.ErrorCode;
import com.cjq.exception.EsSqlParseException;
import com.cjq.plan.logical.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiFunction;

public class HandlerFactory {
    private static volatile HandlerFactory instance;
    
    // Use mapping table to store creation strategies
    private final Map<Class<? extends LogicalPlan>, BiFunction<LogicalPlan, Properties, ResponseHandler>> handlerCreators;

    private HandlerFactory() {
        handlerCreators = new HashMap<>();
        initializeCreators();
    }

    public static HandlerFactory getInstance() {
        if (instance == null) {
            synchronized (HandlerFactory.class) {
                if (instance == null) {
                    instance = new HandlerFactory();
                }
            }
        }
        return instance;
    }

    private void initializeCreators() {
        // Special handling for Query type
        handlerCreators.put(Query.class, (plan, properties) -> {
            Query query = (Query) plan;
            return query.isAgg() ? new AggQueryHandler(query, properties) : new DefaultQueryHandler(query, properties);
        });
        
        // Standard handling for other types
        handlerCreators.put(Delete.class, (plan, properties) -> new DeleteHandler());
        handlerCreators.put(Show.class, (plan, properties) -> new ShowHandler());
        handlerCreators.put(Drop.class, (plan, properties) -> new DropHandler());
        handlerCreators.put(Insert.class, (plan, properties) -> new InsertHandler());
        handlerCreators.put(Update.class, (plan, properties) -> new UpdateHandler());
        handlerCreators.put(UpdateByQuery.class, (plan, properties) -> new UpdateByQueryHandler());
    }

    public ResponseHandler createHandler(LogicalPlan plan, Properties properties) {
        BiFunction<LogicalPlan, Properties, ResponseHandler> creator = handlerCreators.get(plan.getClass());
        
        if (creator != null) {
            return creator.apply(plan, properties);
        }
        
        throw new EsSqlParseException(ErrorCode.UNSUPPORTED_PLAN_TYPE + plan.getClass().getName());
    }
}
