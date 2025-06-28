package com.cjq.action;

import com.cjq.domain.Client;
import com.cjq.exception.EsSqlParseException;
import com.cjq.plan.logical.Delete;
import com.cjq.plan.logical.Drop;
import com.cjq.plan.logical.LogicalPlan;
import com.cjq.plan.logical.Query;

public class ActionPlanFactory {
    private static volatile ActionPlanFactory actionPlanFactory;

    private ActionPlanFactory() {
    }

    public static ActionPlanFactory getInstance() {
        if (actionPlanFactory == null) {
            synchronized (ActionPlanFactory.class) {
                if (actionPlanFactory == null) {
                    actionPlanFactory = new ActionPlanFactory();
                }
            }
        }
        return actionPlanFactory;
    }

    public ActionPlan createAction(LogicalPlan plan) {
        if (plan instanceof Query) {
            if (((Query) plan).isAgg()) {
                return new AggQueryActionPlan(plan);
            } else {
                return new DefaultQueryActionPlan(plan);
            }
        } else if (plan instanceof Delete) {
            return new DeleteActionPlan(plan);
        } else {
            throw new EsSqlParseException("Unsupported logical plan type: " + plan.getClass().getName());
        }
    }
}
