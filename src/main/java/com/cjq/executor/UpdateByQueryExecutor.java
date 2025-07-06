package com.cjq.executor;

import com.cjq.domain.Client;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;

import java.io.IOException;

public class UpdateByQueryExecutor implements Executor {
    private Client client;
    private NodeClient nodeClient;

    public UpdateByQueryExecutor(Client client) {
        this.client = client;
    }

    public UpdateByQueryExecutor(NodeClient nodeClient) {
        this.nodeClient = nodeClient;
    }

    @Override
    public ActionResponse execute(ActionRequest request) throws IOException {
        return client.getClient().updateByQuery((UpdateByQueryRequest) request, RequestOptions.DEFAULT);
    }

    @Override
    public ActionResponse webExecutor(ActionRequest request) {
        return nodeClient.execute(UpdateByQueryAction.INSTANCE, request).actionGet();
    }
}
