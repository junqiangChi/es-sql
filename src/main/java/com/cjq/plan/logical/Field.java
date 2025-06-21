package com.cjq.plan.logical;


import com.cjq.common.FieldFunction;
import com.cjq.exception.EsSqlParseException;

import static com.cjq.common.FieldFunction.*;

public class Field extends LogicalPlan {
    private String fieldName;
    private String alias;
    private boolean isFunction;
    private FieldFunction funcName;
    private boolean isConstant;
    private Object constantValue;

    public Field() {
    }

    public Field(String fieldName, String funcName) {
        this.fieldName = fieldName;
        isFunction = true;
        setFuncName(funcName);
    }

    public Field(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
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

    public boolean isFunction() {
        return isFunction;
    }

    public void setFunction(boolean function) {
        isFunction = function;
    }

    public FieldFunction getFuncName() {
        return funcName;
    }


    public void setFuncName(String funcName) {
        switch (funcName) {
            case "COUNT":
                this.funcName = COUNT;
                break;
            case "SUM":
                this.funcName = SUM;
                break;
            case "AVG":
                this.funcName = AVG;
                break;
            case "MAX":
                this.funcName = MAX;
                break;
            case "MIN":
                this.funcName = MIN;
                break;
            default:
                throw new EsSqlParseException("The function " + funcName + " is not supported.");
        }
    }

    @Override
    public String toString() {
        return "Field{" + "fieldName='" + fieldName + '\'' +
                ", alias='" + alias + '\'' +
                ", funcName='" + funcName + '\'' +
                ", isConstant=" + isConstant +
                ", constantValue=" + constantValue +
                '}';
    }
}
