package com.cjq.executor;

import com.cjq.domain.Client;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.node.NodeClient;

import com.cjq.common.Constant;
import java.io.IOException;

public class ShowExecutor extends BaseExecutor {


    public ShowExecutor(Client client) {
        super(client);
    }

    public ShowExecutor(NodeClient nodeClient) {
        super(nodeClient);
    }

    @Override
    public ActionResponse execute(ActionRequest request) throws IOException {
        validateClient();
        
        GetIndexRequest getIndexRequest = (GetIndexRequest) request;
        String[] requestIndices = getIndexRequest.indices();
        requestIndices = requestIndices.length == 0 ? Constant.DEFAULT_INDICES : requestIndices;
        
        org.elasticsearch.client.indices.GetIndexRequest newGetIndexRequest =
                new org.elasticsearch.client.indices.GetIndexRequest(requestIndices);
        org.elasticsearch.client.indices.GetIndexResponse getIndexResponse =
                client.getClient().indices().get(newGetIndexRequest, RequestOptions.DEFAULT);
        
        String[] indices = getIndexResponse.getIndices();
        return new GetIndexResponse(indices, null, null, null, null, null);
    }

    @Override
    public ActionResponse webExecutor(ActionRequest request) {
        validateNodeClient();
        
        return nodeClient.admin().indices().getIndex((GetIndexRequest) request).actionGet();
    }
}
