package com.cjq.plan.logical;


import java.util.List;

public class OrderBy extends LogicalPlan {
    private List<Sort> sorts;

    public List<Sort> getSorts() {
        return sorts;
    }

    public void setSorts(List<Sort> sorts) {
        this.sorts = sorts;
    }

    @Override
    public String toString() {
        return "OrderBy{" +
                "sorts=" + sorts +
                "plan=" + getPlan() +
                '}';
    }
}
