package com.cjq.action;

import com.cjq.domain.Client;
import com.cjq.exception.EsSqlParseException;
import com.cjq.plan.logical.Drop;
import com.cjq.plan.logical.LogicalPlan;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.RequestOptions;

import java.io.IOException;

public class DropActionPlan implements ActionPlan {
    private Drop drop;
    private Client client;

    public DropActionPlan(LogicalPlan logicalPlan, Client client) {
        if (logicalPlan instanceof Drop) {
            this.drop = (Drop) logicalPlan;
        } else {
            throw new EsSqlParseException("The LogicalPlan is not Drop");
        }
        this.client = client;
    }

    @Override
    public ActionRequest explain() {
        if (drop.isCheckExists()) {
            GetIndexRequest getIndexRequest = new GetIndexRequest(drop.getIndex());
            try {
                boolean indexExist = client.getClient().indices().exists(getIndexRequest, RequestOptions.DEFAULT);
                return indexExist ? new DeleteIndexRequest(drop.getIndex()) : null;
            } catch (IOException e) {
                throw new EsSqlParseException(e);
            }

        }
        return new DeleteIndexRequest(drop.getIndex());
    }

}
