package com.cjq.plan.logical;

import com.cjq.common.WhereOpr;

public class Where {
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

    public void setNextWhere(Where nextWhere) {
        Where where = this.nextWhere;
        while (where != null) {
            where = where.nextWhere;
        }
        this.nextWhere = nextWhere;

    }

    public static class Condition {
        private String field;
        private String keyword;
        private String text;

        public Condition() {
        }

        public Condition(String field, String keyword, String text) {
            this.field = field;
            this.keyword = keyword;
            this.text = text;
        }

        public String getField() {
            return field;
        }

        public void setField(String field) {
            this.field = field;
        }

        public String getKeyword() {
            return keyword;
        }

        public void setKeyword(String keyword) {
            this.keyword = keyword;
        }

        public String getText() {
            return text;
        }

        public void setText(String text) {
            this.text = text;
        }

        @Override
        public String toString() {
            return "Condition{" +
                    "field='" + field + '\'' +
                    ", keyword='" + keyword + '\'' +
                    ", text='" + text + '\'' +
                    '}';
        }
    }

    @Override
    public String toString() {
        return "Where{" +
                "condition=" + condition +
                ", opr='" + opr + '\'' +
                ", nextWhere=" + nextWhere +
                '}';
    }
}
