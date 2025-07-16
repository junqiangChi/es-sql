package com.cjq.handler;

import com.cjq.exception.ElasticsearchExecuteException;
import com.cjq.exception.ExceptionHandler;
import com.cjq.exception.ErrorCode;
import com.cjq.jdbc.HandlerResult;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.rest.RestStatus;

import java.util.ArrayList;

public class UpdateHandler implements ResponseHandler {
    @Override
    public HandlerResult handle(ActionResponse response) {
        UpdateResponse updateResponse = (UpdateResponse) response;
        GetResult getResult = updateResponse.getGetResult();
        RestStatus status = updateResponse.status();
        switch (status) {
            case NOT_FOUND:
                ExceptionHandler.getInstance().handleException(new ElasticsearchExecuteException("DocId: " + getResult.getId() + " is not exist"));
                throw ExceptionHandler.getInstance().createBaseException(new ElasticsearchExecuteException("DocId: " + getResult.getId() + " is not exist"), ErrorCode.EXECUTION_FAILED);
            case OK:
                return new HandlerResult(new ArrayList<>(), new ArrayList<>()).setSuccess(true, 1L).setDml(true);
            default:
                ExceptionHandler.getInstance().handleException(new ElasticsearchExecuteException("Error: " + status.name()));
                throw ExceptionHandler.getInstance().createBaseException(new ElasticsearchExecuteException("Error: " + status.name()), ErrorCode.EXECUTION_FAILED);
        }
    }
}
