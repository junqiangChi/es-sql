package com.cjq.plan.logical;

public class Limit extends LogicalPlan {
    private int num;

    public Limit(int num) {
        this.num = num;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }
}
