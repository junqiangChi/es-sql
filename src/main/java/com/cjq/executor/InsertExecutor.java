package com.cjq.executor;

import com.cjq.domain.Client;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.node.NodeClient;

import java.io.IOException;

public class InsertExecutor implements Executor {
    private Client client;
    private NodeClient nodeClient;

    public InsertExecutor(Client client) {
        this.client = client;
    }

    public InsertExecutor(NodeClient nodeClient) {
        this.nodeClient = nodeClient;
    }

    @Override
    public ActionResponse execute(ActionRequest request) throws IOException {
        return client.getClient().bulk((BulkRequest) request, RequestOptions.DEFAULT);
    }

    @Override
    public ActionResponse webExecutor(ActionRequest request) {
        return nodeClient.bulk((BulkRequest) request).actionGet();
    }
}
