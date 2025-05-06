package com.cjq.plan.logical;

import com.cjq.common.WhereOpr;

public class Where extends LogicalPlan {
    private Condition condition;
    private WhereOpr opr;
    private Where nextWhere;


    public Where() {
    }

    public Where(Condition condition, WhereOpr opr, Where nextWhere) {
        this.condition = condition;
        this.opr = opr;
        this.nextWhere = nextWhere;
    }

    public void setNextWhere(Where nextWhere) {
        Where where = this;
        while (where.nextWhere != null) {
            where = where.nextWhere;
        }
        where.nextWhere = nextWhere;

    }

    @Override
    public String toString() {
        return "Where{" +
                "condition=" + condition +
                ", opr=" + opr +
                ", nextWhere=" + nextWhere +
                '}';
    }

    public Condition getCondition() {
        return condition;
    }

    public void setCondition(Condition condition) {
        this.condition = condition;
    }

    public WhereOpr getOpr() {
        return opr;
    }

    public void setOpr(WhereOpr opr) {
        this.opr = opr;
    }

    public Where getNextWhere() {
        return nextWhere;
    }
}
