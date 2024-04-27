package com.cjq.plan.logical;

public class From {
    private String index;

    public From(String index) {
        this.index = index;
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
