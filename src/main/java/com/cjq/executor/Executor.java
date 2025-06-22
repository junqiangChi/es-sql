package com.cjq.executor;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;

import java.io.IOException;

public interface Executor {
    ActionResponse execute(ActionRequest request) throws IOException;
}
