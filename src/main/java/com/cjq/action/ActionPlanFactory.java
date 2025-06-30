package com.cjq.action;

import com.cjq.exception.EsSqlParseException;
import com.cjq.plan.logical.*;

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
        } else if (plan instanceof Show) {
            return new ShowActionPlan(plan);
        } else if (plan instanceof Drop) {
            return new DropActionPlan(plan);
        }
        throw new EsSqlParseException("Unsupported logical plan type: " + plan.getClass().getName());

    }
}
