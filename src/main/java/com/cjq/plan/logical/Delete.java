package com.cjq.plan.logical;

public class Delete extends LogicalPlan {
    private From from;
    private Where where;

    public Delete(From from, Where where) {
        this.from = from;
        this.where = where;
    }

    public From getFrom() {
        return from;
    }

    public void setFrom(From from) {
        this.from = from;
    }

    public Where getWhere() {
        return where;
    }

    public void setWhere(Where where) {
        this.where = where;
    }

    @Override
    public String toString() {
        return "Delete{" +
                "from=" + from +
                ", where=" + where +
                '}';
    }
}
