package com.cjq.plan.logical;

import java.util.ArrayList;
import java.util.List;

public class Insert extends LogicalPlan {
    private From from;
    private List<Field> fields;
    private List<List<Value>> values;
    private int idPosition = -1;

    public Insert() {
    }

    public Insert(From from) {
        this.from = from;
    }

    public Insert(From from, List<List<Value>> values) {
        this.from = from;
        this.values = values;
    }

    public Insert(From from, List<Field> fields, List<List<Value>> values) {
        this.from = from;
        this.fields = fields;
        this.values = values;
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

    public List<List<Value>> getValues() {
        return values;
    }


    public void setValues(List<List<Value>> values) {
        if (this.values == null) {
            this.values = values;
        } else {
            this.values.addAll(values);
        }
    }

    public void setRow(List<Value> row) {
        if (this.values != null) {
            values.add(row);
        } else {
            this.values = new ArrayList<>();
            values.add(row);
        }
    }

    public int getIdPosition() {
        return idPosition;
    }

    public void setIdPosition(int idPosition) {
        this.idPosition = idPosition;
    }

    @Override
    public String toString() {
        return "Insert{" +
                "from=" + from +
                ", fields=" + fields +
                ", values=" + values +
                '}';
    }
}
