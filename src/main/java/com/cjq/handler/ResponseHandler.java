package com.cjq.handler;

import com.cjq.jdbc.ObjectResult;
import org.elasticsearch.action.ActionResponse;

public interface ResponseHandler {
    ObjectResult handle(ActionResponse response);
}
