package com.cjq.plan.logical;


import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class Query extends LogicalPlan {
    private Select select;
    private From from;
    private Where where;
    private Query subQuery;
    private GroupBy groupBy;
    private boolean isAgg;

    public Query(Select select, From from, Where where) {
        this.select = select;
        this.from = from;
        this.where = where;
    }

    public Query(Select select, From from, Where where, GroupBy groupBy) {
        this.select = select;
        this.from = from;
        this.where = where;
        this.groupBy = groupBy;
        long funcFieldCount = select.getFields().stream().filter(f -> f.getFuncName() != null).count();
        if (groupBy != null || funcFieldCount > 0) {
            isAgg = true;
        }
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

    public GroupBy getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(GroupBy groupBy) {
        this.groupBy = groupBy;
    }

    public boolean isAgg() {
        return isAgg;
    }

    public void setAgg(boolean agg) {
        isAgg = agg;
    }

    @Override
    public String toString() {
        return "Query{" +
                "select=" + select +
                ", from=" + from +
                ", where=" + where +
                ", subQuery=" + subQuery +
                ", groupBy=" + groupBy +
                ", isAgg=" + isAgg +
                ", plan=" + getPlan() +
                '}';
    }
}
