package com.cjq.executor;

import com.cjq.domain.Client;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.node.NodeClient;

import com.cjq.common.Constant;
import java.io.IOException;

/**
 * Base Executor class that provides common functionality
 */
public abstract class BaseExecutor implements Executor {
    
    protected final Client client;
    protected final NodeClient nodeClient;
    
    protected BaseExecutor(Client client) {
        this.client = client;
        this.nodeClient = null;
    }
    
    protected BaseExecutor(NodeClient nodeClient) {
        this.client = null;
        this.nodeClient = nodeClient;
    }
    
    @Override
    public abstract ActionResponse execute(ActionRequest request) throws IOException;
    
    @Override
    public abstract ActionResponse webExecutor(ActionRequest request);
    
    /**
     * Validate that Client is initialized
     */
    protected void validateClient() {
        if (client == null) {
            throw new IllegalStateException(Constant.ERROR_CLIENT_NOT_INITIALIZED);
        }
    }
    
    /**
     * Validate that NodeClient is initialized
     */
    protected void validateNodeClient() {
        if (nodeClient == null) {
            throw new IllegalStateException(Constant.ERROR_NODECLIENT_NOT_INITIALIZED);
        }
    }
} 