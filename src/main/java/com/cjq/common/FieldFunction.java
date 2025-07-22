package com.cjq.common;

public enum FieldFunction {

    COUNT, MAX, MIN, AVG, SUM, CONCAT, CONCAT_WS, SUBSTRING, LENGTH, UPPER, LOWER, TRIM, REPLACE, ABS, CEIL, FLOOR, ROUND,
    MOD, POW, SQRT, RAND, NOW, CURDATE, CURTIME, DATE_FORMAT, DATEDIFF, DAY, MONTH, YEAR, HOUR, MINUTE,
    IF, IFNULL, COALESCE, CASE_WHEN, CAST;

    private boolean isAggFunction;

    FieldFunction() {
    }

    FieldFunction(boolean isAggFunction) {
        this.isAggFunction = isAggFunction;
    }

    public boolean isAggFunction() {
        return isAggFunction;
    }

    public void setAggFunction(boolean aggFunction) {
        isAggFunction = aggFunction;
    }
}
