package com.cjq.handler;

import com.cjq.jdbc.HandlerResult;
import org.elasticsearch.action.ActionResponse;

public interface ResponseHandler {
    HandlerResult handle(ActionResponse response);
}
