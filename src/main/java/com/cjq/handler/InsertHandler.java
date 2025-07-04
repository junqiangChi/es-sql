package com.cjq.handler;

import com.cjq.exception.ElasticsearchExecuteException;
import com.cjq.jdbc.ObjectResult;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkResponse;

import java.util.ArrayList;

public class InsertHandler implements ResponseHandler {
    @Override
    public ObjectResult handle(ActionResponse response) {
        BulkResponse bulkResponse = (BulkResponse) response;
        if (bulkResponse.hasFailures()) {
            throw new ElasticsearchExecuteException("insert failed, error: " + bulkResponse.buildFailureMessage());
        }
        return new ObjectResult(new ArrayList<>(),new ArrayList<>()).setSuccess(true);
    }
}
