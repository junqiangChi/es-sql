package com.cjq.plan.logical;

public class Value extends LogicalPlan{
    private String text;

    public Value() {
    }

    public Value(String text) {
        this.text = text;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return "Value{" +
                "text='" + text + '\'' +
                '}';
    }
}
