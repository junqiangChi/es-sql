package com.cjq.executor;

import com.cjq.domain.Client;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;

import java.io.IOException;

public class DeleteExecutor implements Executor {
    private Client client;
    private NodeClient nodeClient;

    public DeleteExecutor(Client client) {
        this.client = client;
    }

    public DeleteExecutor(NodeClient nodeClient) {
        this.nodeClient = nodeClient;
    }

    @Override
    public ActionResponse execute(ActionRequest request) throws IOException {
        DeleteByQueryRequest deleteByQueryRequest = (DeleteByQueryRequest) request;
        return client.getClient().deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);
    }

    @Override
    public ActionResponse webExecutor(ActionRequest request) {
        return nodeClient.execute(DeleteByQueryAction.INSTANCE, request).actionGet();
    }
}
