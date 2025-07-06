package com.cjq.plan.logical;

import java.util.List;

public class Update extends LogicalPlan {
    private From from;
    private List<Field> fields;
    private List<Value> values;
    private String docId;


    public Update(From from, List<Field> fields, List<Value> values, String docId) {
        this.from = from;
        this.fields = fields;
        this.values = values;
        this.docId = docId;
    }

    public From getFrom() {
        return from;
    }

    public void setFrom(From from) {
        this.from = from;
    }

    public List<Field> getFields() {
        return fields;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

    public List<Value> getValues() {
        return values;
    }

    public void setValues(List<Value> values) {
        this.values = values;
    }

    public String getDocId() {
        return docId;
    }

    public void setDocId(String docId) {
        this.docId = docId;
    }

    @Override
    public String toString() {
        return "Update{" +
                "from=" + from +
                ", fields=" + fields +
                ", values=" + values +
                ", docId='" + docId + '\'' +
                '}';
    }
}
