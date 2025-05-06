package com.cjq.plan.logical;

import com.cjq.common.MultiType;

import java.util.List;

public class Union extends LogicalPlan {
    private List<Query> queries;
    private MultiType multiType;

    public List<Query> getQueries() {
        return queries;
    }

    public void setQueries(List<Query> queries) {
        this.queries = queries;
    }

    public MultiType getMultiType() {
        return multiType;
    }

    public void setMultiType(MultiType multiType) {
        this.multiType = multiType;
    }
}
