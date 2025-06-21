package com.cjq.plan.logical;

import java.util.List;

public class GroupBy extends LogicalPlan{
    private List<Field> groupByFields;

    public GroupBy(List<Field> groupByFields) {
        this.groupByFields = groupByFields;
    }

    public List<Field> getGroupByFields() {
        return groupByFields;
    }

    public void setGroupByFields(List<Field> groupByFields) {
        this.groupByFields = groupByFields;
    }

    @Override
    public String toString() {
        return "GroupBy{" +
                       "groupByFields=" + groupByFields +
                       "} ";
    }
}
