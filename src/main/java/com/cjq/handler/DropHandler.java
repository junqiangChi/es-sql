package com.cjq.handler;

import com.cjq.exception.ElasticsearchExecuteException;
import com.cjq.jdbc.ObjectResult;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;

import java.util.ArrayList;

public class DropHandler implements ResponseHandler {

    @Override
    public ObjectResult handle(ActionResponse response) {
        AcknowledgedResponse acknowledgedResponse = (AcknowledgedResponse) response;
        if (acknowledgedResponse.isAcknowledged()) {
            return new ObjectResult(new ArrayList<>(), new ArrayList<>()).setSuccess(true);
        }
        throw new ElasticsearchExecuteException("Drop failed");
    }
}
