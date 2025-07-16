package com.cjq.action;

import com.cjq.plan.logical.LogicalPlan;
import com.cjq.plan.logical.Show;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;

import java.util.Optional;

public class ShowActionPlan extends BaseActionPlan {
    private final Show show;

    public ShowActionPlan(LogicalPlan plan) {
        super(plan);
        this.show = (Show) plan;
    }

    @Override
    public ActionRequest explain() {
        validateLogicalPlan();
        
        GetIndexRequest getIndexRequest = new GetIndexRequest();
        
        Optional.ofNullable(show.getIndexOrRegex())
                .ifPresent(getIndexRequest::indices);
                
        return getIndexRequest;
    }
}
