package com.cjq.handler;

import com.cjq.jdbc.ObjectResult;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;

import java.util.ArrayList;

public class DropHandler implements ResponseHandler {
    public DropHandler() {
    }

    @Override
    public ObjectResult handle(ActionResponse response) {
        AcknowledgedResponse acknowledgedResponse = (AcknowledgedResponse) response;
        ObjectResult objectResult = new ObjectResult(new ArrayList<>(), new ArrayList<>());
        if (acknowledgedResponse.isAcknowledged()) {
            return objectResult.setSuccess(true);
        } else {
            return objectResult.setSuccess(false);
        }
    }
}
