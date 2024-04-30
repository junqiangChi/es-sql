package com.cjq.plan.logical;

import java.util.List;

public class Select extends LogicalPlan{
   private List<Field> fields;

    public Select(List<Field> fields) {
        this.fields = fields;
    }

    public List<Field> getFields() {
        return fields;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

    @Override
    public String toString() {
        return "Select{" +
                "fields=" + fields +
                '}';
    }
}
