package com.cjq.plan.logical;

public class Field {
    private String field;
    private String alias;

    public Field(String field, String alias) {
        this.field = field;
        this.alias = alias;
    }

    public Field(String field) {
        this.field = field;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    @Override
    public String toString() {
        return "Field{" +
                "field='" + field + '\'' +
                ", alias='" + alias + '\'' +
                '}';
    }
}
