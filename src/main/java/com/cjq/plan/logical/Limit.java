package com.cjq.plan.logical;

public class Limit extends LogicalPlan {
    private Integer from;
    private Integer size;

    public Limit(Integer from, Integer size) {
        this.from = from;
        this.size = size;
    }

    public Integer getFrom() {
        return from;
    }

    public void setFrom(Integer from) {
        this.from = from;
    }

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    @Override
    public String toString() {
        return "Limit{" +
                "from=" + from +
                ", size=" + size +
                '}';
    }
}
