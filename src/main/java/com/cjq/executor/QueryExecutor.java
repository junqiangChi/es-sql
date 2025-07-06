package com.cjq.executor;

import com.cjq.domain.Client;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.node.NodeClient;

import java.io.IOException;

public class QueryExecutor implements Executor {
    private Client client;
    private NodeClient nodeClient;

    public QueryExecutor(Client client) {
        this.client = client;
    }

    public QueryExecutor(NodeClient nodeClient) {
        this.nodeClient = nodeClient;
    }

    @Override
    public ActionResponse execute(ActionRequest request) throws IOException {
        return this.client.getClient().search((SearchRequest) request, RequestOptions.DEFAULT);
    }

    @Override
    public ActionResponse webExecutor(ActionRequest request) {
        return nodeClient.search((SearchRequest) request).actionGet();
    }
}
