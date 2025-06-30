package com.cjq.executor;

import com.cjq.domain.Client;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.client.RequestOptions;

import java.io.IOException;

public class ShowExecutor implements Executor {
    private final Client client;

    public ShowExecutor(Client client) {
        this.client = client;
    }

    @Override
    public ActionResponse execute(ActionRequest request) throws IOException {
        GetIndexRequest getIndexRequest = (GetIndexRequest) request;
        String[] requestIndices = getIndexRequest.indices();
        requestIndices = requestIndices.length == 0 ? new String[]{"*"} : requestIndices;
        org.elasticsearch.client.indices.GetIndexRequest newGetIndexRequest =
                new org.elasticsearch.client.indices.GetIndexRequest(requestIndices);
        org.elasticsearch.client.indices.GetIndexResponse getIndexResponse =
                client.getClient().indices().get(newGetIndexRequest, RequestOptions.DEFAULT);
        String[] indices = getIndexResponse.getIndices();
        return new GetIndexResponse(indices, null, null, null, null, null);
    }
}
