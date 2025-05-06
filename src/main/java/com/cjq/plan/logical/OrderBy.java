package com.cjq.plan.logical;


import java.util.List;

public class OrderBy extends LogicalPlan {
    private List<LogicalPlan> sorts;

    public List<LogicalPlan> getSorts() {
        return sorts;
    }

    public void setSorts(List<LogicalPlan> sorts) {
        this.sorts = sorts;
    }
}
