package com.cjq.executor;

import com.cjq.domain.Client;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;

import java.io.IOException;

public class UpdateByQueryExecutor implements Executor {
    private final Client client;

    public UpdateByQueryExecutor(Client client) {
        this.client = client;
    }

    @Override
    public ActionResponse execute(ActionRequest request) throws IOException {
        return client.getClient().updateByQuery((UpdateByQueryRequest) request, RequestOptions.DEFAULT);
    }
}
