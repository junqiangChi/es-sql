package com.cjq.handler;

import com.cjq.exception.ErrorCode;
import com.cjq.jdbc.HandlerResult;
import org.elasticsearch.action.ActionResponse;

/**
 * Base Handler class that provides common functionality
 */
public abstract class BaseHandler implements ResponseHandler {
    
    @Override
    public abstract HandlerResult handle(ActionResponse response);
    
    /**
     * Validate that response is not null
     */
    protected void validateResponse(ActionResponse response) {
        if (response == null) {
            throw new IllegalArgumentException(ErrorCode.RESPONSE_NULL.getDefaultMessage());
        }
    }
    
    /**
     * Validate response type
     */
    protected <T extends ActionResponse> T validateResponseType(ActionResponse response, Class<T> expectedType) {
        validateResponse(response);
        
        if (!expectedType.isInstance(response)) {
            throw new IllegalArgumentException("Expected response type: " + expectedType.getSimpleName() + 
                    ", but got: " + response.getClass().getSimpleName());
        }
        
        return expectedType.cast(response);
    }
} 