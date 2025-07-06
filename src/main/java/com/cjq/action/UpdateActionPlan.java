package com.cjq.action;

import com.cjq.plan.logical.Field;
import com.cjq.plan.logical.LogicalPlan;
import com.cjq.plan.logical.Update;
import com.cjq.plan.logical.Value;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;

import java.util.HashMap;
import java.util.List;

public class UpdateActionPlan implements ActionPlan {
    private Update update;

    public UpdateActionPlan(LogicalPlan plan) {
        this.update = (Update) plan;
    }

    @Override
    public ActionRequest explain() {
        UpdateRequest updateRequest = new UpdateRequest(update.getFrom().getIndex(), update.getDocId());
        List<Field> fields = update.getFields();
        List<Value> values = update.getValues();
        HashMap<String, Object> doc = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            doc.put(fields.get(i).getFieldName(), values.get(i).getText());
        }
        updateRequest.doc(doc);
        updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        return updateRequest;
    }
}
