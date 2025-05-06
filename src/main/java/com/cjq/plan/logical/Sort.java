package com.cjq.plan.logical;


public class Sort extends LogicalPlan {
    public String field;
    public OrderType orderType;

    public enum OrderType {
        ASC, DESC
    }

    public Sort(String field, OrderType orderType) {
        this.field = field;
        this.orderType = orderType;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public OrderType getOrderType() {
        return orderType;
    }

    public void setOrderType(OrderType orderType) {
        this.orderType = orderType;
    }
}
