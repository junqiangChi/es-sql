package com.cjq.plan.logical;

public class Show extends LogicalPlan {
    private String indexOrRegex;

    public Show() {
    }

    public Show(String indexOrRegex) {
        this.indexOrRegex = indexOrRegex;
    }

    public String getIndexOrRegex() {
        return indexOrRegex;
    }

    public void setIndexOrRegex(String indexOrRegex) {
        this.indexOrRegex = indexOrRegex;
    }

    @Override
    public String toString() {
        return "Show{" +
                "indexOrRegex='" + indexOrRegex + '\'' +
                '}';
    }
}
