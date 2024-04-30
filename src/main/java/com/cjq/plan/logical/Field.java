package com.cjq.plan.logical;

public class Field extends LogicalPlan{
    private String field;
    private String alias;
    private boolean isConstant;

    public Field(String field, String alias) {
        this.field = field;
        this.alias = alias;
    }

    public Field(String field, String alias, boolean isConstant) {
        this.field = field;
        this.alias = alias;
        this.isConstant = isConstant;
    }

    public boolean isConstant() {
        return isConstant;
    }

    public void setConstant(boolean constant) {
        isConstant = constant;
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
            ", isConstant=" + isConstant +
            '}';
    }
}
