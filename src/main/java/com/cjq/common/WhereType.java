package com.cjq.common;

public enum WhereType {

    MATCH("MATCH"),
    TERM(""),
    MULTI_MATCH("MULTI_MATCH"),
    REGEXP("REGEXP"),
    LIKE("LIKE"),
    MATCH_PHRASE("MATCH_PHRASE"),
    EQ("="),
    NE("!="),
    GT(">");

    private String id;

    WhereType(String id) {
        this.id = id;
    }
}
