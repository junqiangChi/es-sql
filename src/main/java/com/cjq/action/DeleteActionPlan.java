package com.cjq.action;

import com.cjq.plan.logical.Delete;
import com.cjq.plan.logical.LogicalPlan;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;

public class DeleteActionPlan extends DefaultQueryActionPlan {
    private final Delete delete;

    public DeleteActionPlan(LogicalPlan plan) {
        this.delete = (Delete) plan;
    }

    @Override
    public ActionRequest explain() {
        setWhere(delete.getWhere());
        DeleteByQueryRequest request = new DeleteByQueryRequest(delete.getFrom().getIndex());
        request.setQuery(searchSourceBuilder.query());
        request.setBatchSize(1000);
        request.setRefresh(true);
        return request;
    }
}
