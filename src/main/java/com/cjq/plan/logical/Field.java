package com.cjq.plan.logical;


import com.cjq.common.FieldFunction;
import com.cjq.exception.EsSqlParseException;

import static com.cjq.common.FieldFunction.*;

public class Field extends LogicalPlan {
    private String fieldName;
    private String alias;
    private boolean isNested;

    public Field() {
    }

    public Field(String fieldName, String alias) {
        this.fieldName = fieldName;
        this.alias = alias;
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

    public boolean isNested() {
        return isNested;
    }

    public void setNested(boolean nested) {
        isNested = nested;
    }

    @Override
    public String toString() {
        return "Field{" +
                "fieldName='" + fieldName + '\'' +
                ", alias='" + alias + '\'' +
                ", isNested=" + isNested +
                '}';
    }
}
