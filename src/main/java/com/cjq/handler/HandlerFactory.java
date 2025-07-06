package com.cjq.handler;

import com.cjq.exception.EsSqlParseException;
import com.cjq.plan.logical.*;

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
        } else if (plan instanceof Delete) {
            return new DeleteHandler();
        } else if (plan instanceof Show) {
            return new ShowHandler();
        } else if (plan instanceof Drop) {
            return new DropHandler();
        } else if (plan instanceof Insert) {
            return new InsertHandler();
        } else if (plan instanceof Update) {
            return new UpdateHandler();
        } else if (plan instanceof UpdateByQuery) {
            return new UpdateByQueryHandler();
        }
        throw new EsSqlParseException("Unsupported plan type: " + plan.getClass().getName());
    }
}
