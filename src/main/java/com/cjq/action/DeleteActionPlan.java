package com.cjq.action;

import com.cjq.plan.logical.Delete;
import com.cjq.plan.logical.LogicalPlan;
import com.cjq.plan.logical.Where;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteActionPlan extends DefaultQueryActionPlan {
    private static final Logger LOG = LoggerFactory.getLogger(DeleteActionPlan.class);
    private final Delete delete;

    public DeleteActionPlan(LogicalPlan plan) {
        this.delete = (Delete) plan;
    }

    @Override
    protected void setWhere(Where where) {
        super.setWhere(where);
    }

    @Override
    public ActionRequest explain() {
        setWhere(delete.getWhere());
        LOG.debug("deleteQuery: {}", searchSourceBuilder);
        DeleteByQueryRequest request = new DeleteByQueryRequest(delete.getFrom().getIndex());
        request.setQuery(searchSourceBuilder.query());
        request.setBatchSize(1000);
        request.setRefresh(true);
        return request;
    }
}
