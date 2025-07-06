package com.cjq.executor;

import com.cjq.domain.Client;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.node.NodeClient;

import java.io.IOException;

public class UpdateExecutor implements Executor {
    private Client client;
    private NodeClient nodeClient;

    public UpdateExecutor(Client client) {
        this.client = client;
    }

    public UpdateExecutor(NodeClient nodeClient) {
        this.nodeClient = nodeClient;
    }

    @Override
    public ActionResponse execute(ActionRequest request) throws IOException {
        return client.getClient().update((UpdateRequest) request, RequestOptions.DEFAULT);
    }

    @Override
    public ActionResponse webExecutor(ActionRequest request) {
        return nodeClient.update((UpdateRequest) request).actionGet();
    }
}
