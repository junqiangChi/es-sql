package com.cjq.handler;

import com.cjq.exception.ElasticsearchExecuteException;
import com.cjq.exception.ExceptionHandler;
import com.cjq.exception.ErrorCode;
import com.cjq.jdbc.HandlerResult;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;

import java.util.ArrayList;

public class DropHandler implements ResponseHandler {

    @Override
    public HandlerResult handle(ActionResponse response) {
        AcknowledgedResponse acknowledgedResponse = (AcknowledgedResponse) response;
        if (acknowledgedResponse.isAcknowledged()) {
            return new HandlerResult(new ArrayList<>(), new ArrayList<>()).setSuccess(true, 0L).setDml(true);
        }
        ExceptionHandler.getInstance().handleException(new ElasticsearchExecuteException("Drop failed"));
        throw ExceptionHandler.getInstance().createBaseException(new ElasticsearchExecuteException("Drop failed"), ErrorCode.EXECUTION_FAILED);
    }
}
