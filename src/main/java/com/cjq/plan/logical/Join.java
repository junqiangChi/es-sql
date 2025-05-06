package com.cjq.plan.logical;


import java.util.List;

/**
 * @author: Julian Chi
 * @date: 2024/8/22 16:35
 * @description:
 */

public class Join extends LogicalPlan {
    private List<From> froms;
    private List<Condition> joinOn;

    public Join(List<From> froms, List<Condition> joinOn) {
        this.froms = froms;
        this.joinOn = joinOn;
    }

    public Join(LogicalPlan plan, List<From> froms, List<Condition> joinOn) {
        super(plan);
        this.froms = froms;
        this.joinOn = joinOn;
    }

    public List<From> getFroms() {
        return froms;
    }

    public void setFroms(List<From> froms) {
        this.froms = froms;
    }

    public List<Condition> getJoinOn() {
        return joinOn;
    }

    public void setJoinOn(List<Condition> joinOn) {
        this.joinOn = joinOn;
    }
}
