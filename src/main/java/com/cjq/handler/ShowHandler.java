package com.cjq.handler;

import com.cjq.jdbc.HandlerResult;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;

import com.cjq.common.Constant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ShowHandler extends BaseHandler {

    
    @Override
    public HandlerResult handle(ActionResponse response) {
        GetIndexResponse getIndexResponse = validateResponseType(response, GetIndexResponse.class);
        String[] indices = getIndexResponse.getIndices();
        
        List<List<Object>> lines = Arrays.stream(indices)
                .filter(index -> !index.startsWith(Constant.HIDDEN_INDEX_PREFIX))
                .map(index -> Collections.singletonList((Object) index))
                .collect(Collectors.toList());
                
        return new HandlerResult(Collections.singletonList(Constant.INDEX_COLUMN), lines);
    }
}
