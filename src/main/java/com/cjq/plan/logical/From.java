package com.cjq.plan.logical;

public class From extends LogicalPlan{
    private String index;
    private String alias;

    public From() {
    }

    public From(String index) {
        this.index = index;
    }

    public From(String index, String alias) {
        this.index = index;
        this.alias = alias;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    @Override
    public String toString() {
        return "From{" +
                "index='" + index + '\'' +
                '}';
    }
}
