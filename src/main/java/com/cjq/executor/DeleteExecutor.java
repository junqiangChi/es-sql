package com.cjq.executor;

import com.cjq.domain.Client;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;

import java.io.IOException;

public class DeleteExecutor implements Executor {
    private final Client client;

    public DeleteExecutor(Client client) {
        this.client = client;
    }

    @Override
    public ActionResponse execute(ActionRequest request) throws IOException {
        DeleteByQueryRequest deleteByQueryRequest = (DeleteByQueryRequest) request;
        return client.getClient().deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);
    }
}
