package com.cjq.handler;

import com.cjq.exception.ElasticsearchExecuteException;
import com.cjq.exception.ExceptionHandler;
import com.cjq.exception.ErrorCode;
import com.cjq.jdbc.HandlerResult;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkResponse;

import java.util.ArrayList;
import java.util.Arrays;

public class InsertHandler implements ResponseHandler {
    @Override
    public HandlerResult handle(ActionResponse response) {
        BulkResponse bulkResponse = (BulkResponse) response;
        if (bulkResponse.hasFailures()) {
            ExceptionHandler.getInstance().handleException(new ElasticsearchExecuteException("insert failed, error: " + bulkResponse.buildFailureMessage()));
            throw ExceptionHandler.getInstance().createBaseException(new ElasticsearchExecuteException("insert failed, error: " + bulkResponse.buildFailureMessage()), ErrorCode.EXECUTION_FAILED);
        }
        long rows = bulkResponse.getItems().length;
        return new HandlerResult(new ArrayList<>(), new ArrayList<>()).setSuccess(true, rows).setDml(true);
    }
}
