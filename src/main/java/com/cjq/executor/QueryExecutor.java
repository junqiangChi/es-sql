package com.cjq.executor;

import com.cjq.domain.Client;
import com.cjq.exception.ErrorCode;
import com.cjq.exception.ExceptionHandler;
import com.cjq.exception.ExceptionUtils;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.node.NodeClient;

import java.io.IOException;

/**
 * Executor for query operations
 * Handles search requests with unified exception handling
 */
public class QueryExecutor implements Executor {
    private final Client client;
    private final NodeClient nodeClient;

    /**
     * Constructs a new QueryExecutor with REST client
     *
     * @param client the Elasticsearch client
     */
    public QueryExecutor(Client client) {
        ExceptionUtils.validateNotNull(client, "Client");
        this.client = client;
        this.nodeClient = null;
    }

    /**
     * Constructs a new QueryExecutor with Node client
     *
     * @param nodeClient the Node client
     */
    public QueryExecutor(NodeClient nodeClient) {
        ExceptionUtils.validateNotNull(nodeClient, "NodeClient");
        this.nodeClient = nodeClient;
        this.client = null;
    }

    @Override
    public ActionResponse execute(ActionRequest request) throws IOException {
        ExceptionUtils.validateNotNull(request, "Request");
        ExceptionUtils.validateCondition(request instanceof SearchRequest, "Request must be a SearchRequest");

        if (client == null) {
            throw ExceptionUtils.createExecuteException(ErrorCode.CONFIGURATION_ERROR, "REST client not available");
        }

        try {
            return client.getClient().search((SearchRequest) request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            ExceptionHandler.getInstance().handleException(e);
            throw ExceptionHandler.getInstance().createBaseException(e, ErrorCode.EXECUTION_FAILED, "Failed to execute search request");
        }
    }

    @Override
    public ActionResponse webExecutor(ActionRequest request) {
        ExceptionUtils.validateNotNull(request, "Request");
        ExceptionUtils.validateCondition(request instanceof SearchRequest, "Request must be a SearchRequest");

        if (nodeClient == null) {
            throw ExceptionUtils.createExecuteException(ErrorCode.CONFIGURATION_ERROR, "Node client not available");
        }

        return ExceptionUtils.executeWithExceptionHandling(
                () -> nodeClient.search((SearchRequest) request).actionGet(),
                ErrorCode.EXECUTION_FAILED,
                "Failed to execute web search request"
        );
    }
}
