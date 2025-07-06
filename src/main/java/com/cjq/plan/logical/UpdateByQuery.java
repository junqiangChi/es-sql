package com.cjq.plan.logical;

import java.util.List;

public class UpdateByQuery extends LogicalPlan{
    private From from;
    private List<Field> fields;
    private List<Value> values;
    private Where where;



    public UpdateByQuery(From from, List<Field> fields, List<Value> values , Where where) {
        this.from = from;
        this.fields = fields;
        this.values = values;
        this.where = where;

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

    public Where getWhere() {
        return where;
    }

    public void setWhere(Where where) {
        this.where = where;
    }

    @Override
    public String toString() {
        return "UpdateByQuery{" +
                "from=" + from +
                ", fields=" + fields +
                ", values=" + values +
                ", where=" + where +
                '}';
    }
}
