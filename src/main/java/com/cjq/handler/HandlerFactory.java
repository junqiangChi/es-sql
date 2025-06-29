package com.cjq.handler;

import com.cjq.exception.EsSqlParseException;
import com.cjq.plan.logical.Delete;
import com.cjq.plan.logical.LogicalPlan;
import com.cjq.plan.logical.Query;

import java.util.Properties;

public class HandlerFactory {
    private static HandlerFactory instance = new HandlerFactory();

    private HandlerFactory() {
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

    public ResponseHandler createHandler(LogicalPlan plan, Properties properties) {
        if (plan instanceof Query) {
            if (((Query) plan).isAgg()) {
                return new AggQueryHandler((Query) plan, properties);
            } else {
                return new DefaultQueryHandler((Query) plan, properties);
            }
        }  else if (plan instanceof Delete) {
            return new DeleteHandler();
        }
        throw new EsSqlParseException("Unsupported plan type: " + plan.getClass().getName());
    }
}
