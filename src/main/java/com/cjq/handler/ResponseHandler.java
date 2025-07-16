package com.cjq.handler;

import com.cjq.jdbc.HandlerResult;
import org.elasticsearch.action.ActionResponse;

/**
 * Response Handler interface for Elasticsearch operations
 * Defines the contract for processing Elasticsearch action responses
 * Converts Elasticsearch responses into standardized HandlerResult objects
 */
public interface ResponseHandler {
    
    /**
     * Handles an Elasticsearch action response and converts it to a HandlerResult
     * This method processes the raw Elasticsearch response and extracts relevant data
     * into a format suitable for JDBC result sets
     * 
     * @param response the Elasticsearch action response to process
     * @return a HandlerResult containing the processed data
     */
    HandlerResult handle(ActionResponse response);
}
