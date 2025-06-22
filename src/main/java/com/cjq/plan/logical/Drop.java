package com.cjq.plan.logical;


public class Drop extends LogicalPlan {
    private String index;
    private boolean isCheckExists;

    public Drop(String index) {
        this.index = index;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public boolean isCheckExists() {
        return isCheckExists;
    }

    public void setCheckExists(boolean checkExists) {
        isCheckExists = checkExists;
    }

    @Override
    public String toString() {
        return "Drop{" +
                "index='" + index + '\'' +
                ", isExist=" + isCheckExists +
                '}';
    }
}
