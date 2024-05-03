package com.cjq.plan.logical;

import java.util.function.BiFunction;

public class LogicalPlan {
    private LogicalPlan plan;

    public LogicalPlan() {
    }

    public LogicalPlan(LogicalPlan plan) {
        this.plan = plan;
    }

    public LogicalPlan getPlan() {
        return plan;
    }

    public void setPlan(LogicalPlan nextPlan) {
        LogicalPlan plan = this;
        while(plan.plan != null){
            plan = plan.plan;
        }
        plan.plan = nextPlan;
    }
    public <C> LogicalPlan optionalMap(C ctx, BiFunction<C, LogicalPlan, LogicalPlan> f) {
        if (ctx != null)
            return f.apply(ctx, plan);
        return plan;
    }
}
