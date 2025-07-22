package com.cjq.plan.logical;


import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 * Represents a SQL query in the logical plan
 * Contains all the components of a SELECT query including SELECT, FROM, WHERE, and GROUP BY clauses
 * Supports both simple queries and aggregate queries with grouping
 */
public class Query extends LogicalPlan {

    /**
     * The SELECT clause containing the fields to retrieve
     */
    private Select select;

    /**
     * The FROM clause specifying the data source
     */
    private From from;

    /**
     * The WHERE clause containing filtering conditions
     */
    private Where where;

    /**
     * Subquery if this query contains a nested query
     */
    private Query subQuery;

    /**
     * The GROUP BY clause for aggregate queries
     */
    private GroupBy groupBy;

    /**
     * Flag indicating if this is an aggregate query
     */
    private boolean isAgg;

    /**
     * Constructs a simple query without grouping
     *
     * @param select the SELECT clause
     * @param from   the FROM clause
     * @param where  the WHERE clause
     */
    public Query(Select select, From from, Where where) {
        this.select = select;
        this.from = from;
        this.where = where;
    }

    /**
     * Constructs a query that may include grouping
     * Automatically determines if this is an aggregate query based on grouping or function fields
     *
     * @param select  the SELECT clause
     * @param from    the FROM clause
     * @param where   the WHERE clause
     * @param groupBy the GROUP BY clause
     */
    public Query(Select select, From from, Where where, GroupBy groupBy) {
        this.select = select;
        this.from = from;
        this.where = where;
        this.groupBy = groupBy;

        // Determine if this is an aggregate query
        long funcFieldCount = select.getFields().stream().filter(f -> f instanceof FunctionField &&
                ((FunctionField) f).getFuncName().isAggFunction()).count();
        if (groupBy != null || funcFieldCount > 0) {
            isAgg = true;
        }
    }

    /**
     * Gets the SELECT clause
     *
     * @return the SELECT clause
     */
    public Select getSelect() {
        return select;
    }

    /**
     * Sets the SELECT clause
     *
     * @param select the SELECT clause to set
     */
    public void setSelect(Select select) {
        this.select = select;
    }

    /**
     * Gets the FROM clause
     *
     * @return the FROM clause
     */
    public From getFrom() {
        return from;
    }

    /**
     * Sets the FROM clause
     *
     * @param from the FROM clause to set
     */
    public void setFrom(From from) {
        this.from = from;
    }

    /**
     * Gets the WHERE clause
     *
     * @return the WHERE clause
     */
    public Where getWhere() {
        return where;
    }

    /**
     * Sets the WHERE clause
     *
     * @param where the WHERE clause to set
     */
    public void setWhere(Where where) {
        this.where = where;
    }

    /**
     * Gets the subquery if this query contains a nested query
     *
     * @return the subquery, or null if there is no subquery
     */
    public Query getSubQuery() {
        return subQuery;
    }

    /**
     * Sets the subquery
     *
     * @param subQuery the subquery to set
     */
    public void setSubQuery(Query subQuery) {
        this.subQuery = subQuery;
    }

    /**
     * Gets the GROUP BY clause
     *
     * @return the GROUP BY clause, or null if there is no grouping
     */
    public GroupBy getGroupBy() {
        return groupBy;
    }

    /**
     * Sets the GROUP BY clause
     *
     * @param groupBy the GROUP BY clause to set
     */
    public void setGroupBy(GroupBy groupBy) {
        this.groupBy = groupBy;
    }

    /**
     * Checks if this is an aggregate query
     *
     * @return true if this is an aggregate query, false otherwise
     */
    public boolean isAgg() {
        return isAgg;
    }

    /**
     * Sets the aggregate flag
     *
     * @param agg true to mark as aggregate query, false otherwise
     */
    public void setAgg(boolean agg) {
        isAgg = agg;
    }

    /**
     * Returns a string representation of this query
     *
     * @return string representation including all query components
     */
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
