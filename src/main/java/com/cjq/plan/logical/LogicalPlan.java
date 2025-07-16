package com.cjq.plan.logical;


import java.util.function.BiFunction;

/**
 * Base class for all logical plan nodes in the query execution plan
 * Represents a node in the logical query tree that can be chained together
 * to form complex query execution plans
 */
public class LogicalPlan {
    
    /** The next logical plan in the execution chain */
    private LogicalPlan plan;

    /**
     * Default constructor for LogicalPlan
     */
    public LogicalPlan() {
    }

    /**
     * Constructs a LogicalPlan with a specified next plan
     * 
     * @param plan the next logical plan in the execution chain
     */
    public LogicalPlan(LogicalPlan plan) {
        this.plan = plan;
    }

    /**
     * Sets the next plan in the execution chain
     * Traverses to the end of the current chain and appends the new plan
     * 
     * @param nextPlan the plan to append to the end of the current chain
     */
    public void setPlan(LogicalPlan nextPlan) {
        LogicalPlan plan = this;
        while (plan.plan != null) {
            plan = plan.plan;
        }
        plan.plan = nextPlan;
    }

    /**
     * Conditionally applies a transformation function to the plan
     * If the context is not null, applies the function; otherwise returns the current plan
     * 
     * @param ctx the context object to check for null
     * @param f the transformation function to apply
     * @param <C> the type of the context object
     * @return the transformed plan or the current plan if context is null
     */
    public <C> LogicalPlan optionalMap(C ctx, BiFunction<C, LogicalPlan, LogicalPlan> f) {
        if (ctx != null)
            return f.apply(ctx, plan);
        return plan;
    }

    /**
     * Gets the next plan in the execution chain
     * 
     * @return the next logical plan, or null if this is the end of the chain
     */
    public LogicalPlan getPlan() {
        return plan;
    }
}
