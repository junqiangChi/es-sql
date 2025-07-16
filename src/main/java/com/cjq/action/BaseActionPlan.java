package com.cjq.action;

import com.cjq.common.Constant;
import com.cjq.plan.logical.LogicalPlan;
import org.elasticsearch.action.ActionRequest;

/**
 * Base ActionPlan class that provides common functionality
 * Abstract base class for all action plan implementations
 * Provides shared validation and access to the logical plan
 */
public abstract class BaseActionPlan implements ActionPlan {
    
    /** The logical plan to be converted into an Elasticsearch action request */
    protected final LogicalPlan logicalPlan;
    
    /**
     * Constructs a base action plan with the specified logical plan
     * 
     * @param logicalPlan the logical plan to be processed
     */
    protected BaseActionPlan(LogicalPlan logicalPlan) {
        this.logicalPlan = logicalPlan;
    }
    
    /**
     * Converts the logical plan into an Elasticsearch action request
     * Must be implemented by subclasses to handle specific operation types
     * 
     * @return the Elasticsearch action request to be executed
     */
    @Override
    public abstract ActionRequest explain();
    
    /**
     * Gets the logical plan associated with this action plan
     * 
     * @return the logical plan
     */
    protected LogicalPlan getLogicalPlan() {
        return logicalPlan;
    }
    
    /**
     * Validates that the logical plan is not null
     * Throws an IllegalArgumentException if the logical plan is null
     * 
     * @throws IllegalArgumentException if logical plan is null
     */
    protected void validateLogicalPlan() {
        if (logicalPlan == null) {
            throw new IllegalArgumentException(Constant.ERROR_LOGICAL_PLAN_NULL);
        }
    }
} 