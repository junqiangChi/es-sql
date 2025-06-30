package com.cjq.action;

import com.cjq.plan.logical.Drop;
import com.cjq.plan.logical.LogicalPlan;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;

public class DropActionPlan implements ActionPlan {
    private Drop drop;

    public DropActionPlan(LogicalPlan plan) {
        this.drop = (Drop) plan;
    }

    @Override
    public ActionRequest explain() {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest();
        return deleteIndexRequest.indices(drop.getIndex());
    }
}
