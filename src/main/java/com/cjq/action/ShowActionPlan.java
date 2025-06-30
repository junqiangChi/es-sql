package com.cjq.action;

import com.cjq.plan.logical.LogicalPlan;
import com.cjq.plan.logical.Show;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;

public class ShowActionPlan implements ActionPlan {
    private Show show;

    public ShowActionPlan(LogicalPlan plan) {
        this.show = (Show) plan;
    }

    @Override
    public ActionRequest explain() {
        GetIndexRequest getIndexRequest = new GetIndexRequest();
        if (show.getIndexOrRegex() != null) {
            getIndexRequest.indices(show.getIndexOrRegex());
            return getIndexRequest;
        }
        return getIndexRequest;
    }
}
