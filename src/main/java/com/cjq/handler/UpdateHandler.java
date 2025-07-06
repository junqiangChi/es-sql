package com.cjq.handler;

import com.cjq.exception.ElasticsearchExecuteException;
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
                throw new ElasticsearchExecuteException("DocId: " + getResult.getId() + " is not exist");
            case OK:
                return new HandlerResult(new ArrayList<>(), new ArrayList<>()).setSuccess(true);
            default:
                throw new ElasticsearchExecuteException("Error: " + status.name());
        }


    }
}
