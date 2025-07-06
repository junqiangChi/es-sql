package com.cjq.handler;

import com.cjq.exception.ElasticsearchExecuteException;
import com.cjq.jdbc.HandlerResult;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkResponse;

import java.util.ArrayList;

public class InsertHandler implements ResponseHandler {
    @Override
    public HandlerResult handle(ActionResponse response) {
        BulkResponse bulkResponse = (BulkResponse) response;
        if (bulkResponse.hasFailures()) {
            throw new ElasticsearchExecuteException("insert failed, error: " + bulkResponse.buildFailureMessage());
        }
        return new HandlerResult(new ArrayList<>(),new ArrayList<>()).setSuccess(true);
    }
}
