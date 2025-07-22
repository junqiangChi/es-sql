package com.cjq.plan.logical;

public class ConstantField extends Field {
    private Object constantValue;

    public ConstantField(String fieldName, Object constantValue) {
        super(fieldName);
        this.constantValue = constantValue;
    }

    public Object getConstantValue() {
        return constantValue;
    }

    public void setConstantValue(Object constantValue) {
        this.constantValue = constantValue;
    }

    @Override
    public String toString() {
        return "ConstantField{" +
                "constantValue=" + constantValue +
                ", fieldName='" + super.getFieldName() + '\'' +
                '}';
    }
}
