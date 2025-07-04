package com.cjq.executor;

import com.cjq.domain.Client;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.client.RequestOptions;

import java.io.IOException;

public class InsertExecutor implements Executor{
    private final Client client;

    public InsertExecutor(Client client) {
        this.client = client;
    }

    @Override
    public ActionResponse execute(ActionRequest request) throws IOException {
        return client.getClient().bulk((BulkRequest) request, RequestOptions.DEFAULT);
    }
}
