package com.cjq.executor;

import com.cjq.domain.Client;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;

import java.io.IOException;

public class QueryExecutor implements Executor {
    private final Client client;

    public QueryExecutor(Client client) {
        this.client = client;
    }

    @Override
    public ActionResponse execute(ActionRequest request) throws IOException {
        return this.client.getClient().search((SearchRequest) request, RequestOptions.DEFAULT);
    }
}
