package com.cjq.executor;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;

import java.io.IOException;

/**
 * Executor interface for Elasticsearch operations
 * Defines the contract for executing Elasticsearch action requests
 * Provides both standard execution and web-based execution methods
 */
public interface Executor {
    
    /**
     * Executes an Elasticsearch action request using the standard client
     * This method may throw IOException for network or connection issues
     * 
     * @param request the Elasticsearch action request to execute
     * @return the response from Elasticsearch
     * @throws IOException if an I/O error occurs during execution
     */
    ActionResponse execute(ActionRequest request) throws IOException;

    /**
     * Executes an Elasticsearch action request using the web client
     * This method is typically used for web-based operations and doesn't throw checked exceptions
     * 
     * @param request the Elasticsearch action request to execute
     * @return the response from Elasticsearch
     */
    ActionResponse webExecutor(ActionRequest request);
}
