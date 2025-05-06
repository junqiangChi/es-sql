package com.cjq.plan.logical;


public class Query extends LogicalPlan {
    private Select select;
    private From from;
    private Where where;
    private Query subQuery;

    public Query(Select select, From from, Where where) {
        this.select = select;
        this.from = from;
        this.where = where;
    }

    @Override
    public String toString() {
        return "Query{" +
                "select=" + select +
                ", from=" + from +
                ", where=" + where +
                '}';
    }

    public Select getSelect() {
        return select;
    }

    public void setSelect(Select select) {
        this.select = select;
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

    public Query getSubQuery() {
        return subQuery;
    }

    public void setSubQuery(Query subQuery) {
        this.subQuery = subQuery;
    }
}
