package com.cjq.plan.logical;


public class Value extends LogicalPlan {
    private Object text;

    public Value() {
    }

    public Value(Object text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return "Value{" +
                "text='" + text + '\'' +
                '}';
    }

    public Object getText() {
        return text;
    }

    public void setText(Object text) {
        this.text = text;
    }
}
