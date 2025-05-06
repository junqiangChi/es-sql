package com.cjq.action;

import com.cjq.plan.logical.LogicalPlan;
import org.elasticsearch.action.ActionRequest;

public abstract class BaseAction {
    public abstract ActionRequest buildRequest(LogicalPlan logicalPlan);
}
