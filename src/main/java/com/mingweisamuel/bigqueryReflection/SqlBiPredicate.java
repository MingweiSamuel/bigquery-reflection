package com.mingweisamuel.bigqueryReflection;

public enum SqlBiPredicate {
    LESS_THAN("<"),
    LESS_THAN_OR_EQUAL_TO("<="),
    EQUAL_TO("="),
    NOT_EQUAL_TO("<>"),
    GREATER_THAN_OR_EQUAL_TO(">="),
    GREATER_THAN(">");

    public final String op;
    SqlBiPredicate(String op) {
        this.op = op;
    }
}
