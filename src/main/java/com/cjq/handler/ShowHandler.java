package com.cjq.handler;

import com.cjq.jdbc.HandlerResult;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ShowHandler implements ResponseHandler {
    @Override
    public HandlerResult handle(ActionResponse response) {
        GetIndexResponse getIndexResponse = (GetIndexResponse) response;
        String[] indices = getIndexResponse.getIndices();
        List<List<Object>> lines = Arrays.stream(indices)
                .filter(index -> !index.startsWith("."))
                .map(i -> {
                    List<Object> objects = new ArrayList<>();
                    objects.add(i);
                    return objects;
                })
                .collect(Collectors.toList());
        return new HandlerResult(Collections.singletonList("index"), lines);
    }
}
