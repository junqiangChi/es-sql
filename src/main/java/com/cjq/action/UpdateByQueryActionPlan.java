package com.cjq.action;

import com.cjq.plan.logical.Field;
import com.cjq.plan.logical.LogicalPlan;
import com.cjq.plan.logical.UpdateByQuery;
import com.cjq.plan.logical.Value;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

public class UpdateByQueryActionPlan extends DefaultQueryActionPlan implements ActionPlan {

    private UpdateByQuery updateByQuery;

    public UpdateByQueryActionPlan(LogicalPlan plan) {
        this.updateByQuery = (UpdateByQuery) plan;
    }

    @Override
    public ActionRequest explain() {
        setWhere(updateByQuery.getWhere());
        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest(updateByQuery.getFrom().getIndex());
        updateByQueryRequest.setQuery(searchSourceBuilder.query());
        Script script = new Script(
                ScriptType.INLINE,
                "painless",
                getScript(),
                Collections.emptyMap()
        );
        updateByQueryRequest.setScript(script);
        updateByQueryRequest.setConflicts("proceed");
        updateByQueryRequest.setRefresh(true);
        return updateByQueryRequest;
    }

    private String getScript() {
        List<Field> fields = updateByQuery.getFields();
        List<Value> values = updateByQuery.getValues();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.size(); i++) {
            sb.append("ctx._source.").append(fields.get(i).getFieldName());
            Object text = values.get(i).getText();
            if (text instanceof Integer || text instanceof Double || text instanceof Float || text instanceof BigDecimal) {
                sb.append(" = ")
                        .append(values.get(i).getText())
                        .append(";\n");
            } else {
                sb.append(" = '")
                        .append(values.get(i).getText())
                        .append("';\n");
            }
        }
        return sb.toString();
    }
}
