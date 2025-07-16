package com.cjq.action;

import org.elasticsearch.action.ActionRequest;

/**
 * Action Plan interface for Elasticsearch operations
 * Defines the contract for converting logical plans into Elasticsearch action requests
 * Each implementation handles a specific type of SQL operation (SELECT, INSERT, UPDATE, etc.)
 */
public interface ActionPlan {
    
    /**
     * Converts the logical plan into an Elasticsearch action request
     * This method should analyze the logical plan and create the appropriate
     * Elasticsearch request object for execution
     * 
     * @return the Elasticsearch action request to be executed
     */
    ActionRequest explain();
}
