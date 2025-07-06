package com.cjq.handler;

import com.cjq.jdbc.HandlerResult;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.index.reindex.BulkByScrollResponse;

import java.util.ArrayList;

public class DeleteHandler implements ResponseHandler {
    @Override
    public HandlerResult handle(ActionResponse response) {
        BulkByScrollResponse bulkByScrollResponse = (BulkByScrollResponse) response;
        long deleted = bulkByScrollResponse.getDeleted();
        boolean isSuccess = deleted > 0;
        ArrayList<String> headers = new ArrayList<>();
        return new HandlerResult(headers,new ArrayList<>()).setSuccess(isSuccess);
    }
}
