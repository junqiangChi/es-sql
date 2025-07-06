package com.cjq.executor;

import com.cjq.domain.Client;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;

import java.io.IOException;

public class UpdateExecutor implements Executor {
    private final Client client;

    public UpdateExecutor(Client client) {
        this.client = client;
    }

    @Override
    public ActionResponse execute(ActionRequest request) throws IOException {
        return client.getClient().update((UpdateRequest) request, RequestOptions.DEFAULT);
    }
}
