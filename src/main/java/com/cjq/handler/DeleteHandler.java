package com.cjq.handler;

import com.cjq.jdbc.ObjectResult;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.index.reindex.BulkByScrollResponse;

import java.util.ArrayList;

public class DeleteHandler implements ResponseHandler {
    @Override
    public ObjectResult handle(ActionResponse response) {
        BulkByScrollResponse bulkByScrollResponse = (BulkByScrollResponse) response;
        long deleted = bulkByScrollResponse.getDeleted();
        boolean isSuccess = deleted > 0;
        ArrayList<String> headers = new ArrayList<>();
        return new ObjectResult(headers,new ArrayList<>()).setSuccess(isSuccess);
    }
}
