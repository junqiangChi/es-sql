package com.cjq.plan.logical;

import com.cjq.common.OperatorSymbol;
import com.cjq.exception.EsSqlParseException;

public class Condition {
    private String field;
    private OperatorSymbol opera;
    private Value value;

    public Condition(String field, String operaStr, Value value) {
        this.field = field;
        this.value = value;
        switch (operaStr.toUpperCase()) {
            case "=":
                this.opera = OperatorSymbol.EQ;
                break;
            case ">":
                this.opera = OperatorSymbol.GT;
                break;
            case "<":
                this.opera = OperatorSymbol.LT;
                break;
            case ">=":
                this.opera = OperatorSymbol.GTE;
                break;
            case "<=":
                this.opera = OperatorSymbol.LTE;
                break;
            case "<>":
                this.opera = OperatorSymbol.N;
                break;
            case "LIKE":
                this.opera = OperatorSymbol.LIKE;
                break;
            case "NOT":
                this.opera = OperatorSymbol.N;
                break;
            case "NOT LIKE":
                this.opera = OperatorSymbol.NLIKE;
                break;
            case "IS":
                this.opera = OperatorSymbol.IS;
                break;
            case "IS NOT":
                this.opera = OperatorSymbol.ISN;
                break;
            case "NOT IN":
                this.opera = OperatorSymbol.NIN;
                break;
            case "IN":
                this.opera = OperatorSymbol.IN;
                break;
            case "BETWEEN":
                this.opera = OperatorSymbol.BETWEEN;
                break;
            case "NOT BETWEEN":
                this.opera = OperatorSymbol.NBETWEEN;
                break;
            case "GEO_INTERSECTS":
                this.opera = OperatorSymbol.GEO_INTERSECTS;
                break;
            case "GEO_BOUNDING_BOX":
                this.opera = OperatorSymbol.GEO_BOUNDING_BOX;
                break;
            case "GEO_DISTANCE":
                this.opera = OperatorSymbol.GEO_DISTANCE;
                break;
            case "GEO_POLYGON":
                this.opera = OperatorSymbol.GEO_POLYGON;
                break;
            case "NESTED":
                this.opera = OperatorSymbol.NESTED_COMPLEX;
                break;
            case "NOT NESTED":
                this.opera = OperatorSymbol.NNESTED_COMPLEX;
                break;
            case "CHILDREN":
                this.opera = OperatorSymbol.CHILDREN_COMPLEX;
                break;
            case "SCRIPT":
                this.opera = OperatorSymbol.SCRIPT;
                break;
            case "REGEXP":
                this.opera = OperatorSymbol.REGEXP;
                break;
            case "MATCH":
                this.opera = OperatorSymbol.MATCH;
                break;
            case "MATCH_PHRASE":
                this.opera = OperatorSymbol.MATCH_PHRASE;
                break;
            case "TERM":
                this.opera = OperatorSymbol.TERM;
                break;
            default:
                throw new EsSqlParseException(operaStr + " is err!");
        }
    }

/*        public void getOperatorSymbol() {
            switch (opera) {
                case EQ:
                    return;
                case GT:
                    return;
                case LT:
                    return;
                case GTE:
                    return;
                case LTE:
                    return;
                case N:
                    return;
                case IS:
                    return;

                case ISN:
                    return;
                default:
                    throw new EsSqlParseException(opera + " is err!");
            }
        }*/


    public OperatorSymbol getOpera() {
        return opera;
    }

    public void setOpera(OperatorSymbol opera) {
        this.opera = opera;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public Value getValue() {
        return value;
    }

    public void setValue(Value value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "Condition{" +
                "field='" + field + '\'' +
                ", keyword='" + opera + '\'' +
                ", text='" + value + '\'' +
                '}';
    }
}