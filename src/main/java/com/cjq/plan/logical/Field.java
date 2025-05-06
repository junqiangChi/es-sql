package com.cjq.plan.logical;


public class Field extends LogicalPlan {
    private String field;
    private String alias;
    private boolean isConstant;
    private Object constantValue;

    public Field(String field, String alias, boolean isConstant) {
        this.field = field;
        this.alias = alias;
        this.isConstant = isConstant;
    }

    public Field(String field, String alias, boolean isConstant, Object constantValue) {
        this.field = field;
        this.alias = alias;
        this.isConstant = isConstant;
        this.constantValue = constantValue;
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

    public boolean isConstant() {
        return isConstant;
    }

    public void setConstant(boolean constant) {
        isConstant = constant;
    }

    public Object getConstantValue() {
        return constantValue;
    }

    public void setConstantValue(Object constantValue) {
        this.constantValue = constantValue;
    }
}
