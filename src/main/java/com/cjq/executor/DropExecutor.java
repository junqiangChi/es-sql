package com.cjq.executor;

import com.cjq.domain.Client;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;

import java.io.IOException;

public class DropExecutor implements Executor {
    private final Client client;

    public DropExecutor(Client client) {
        this.client = client;
    }

    @Override
    public ActionResponse execute(ActionRequest request) throws IOException {
        return client.getClient().indices().delete((DeleteIndexRequest) request, RequestOptions.DEFAULT);
    }
}
