package com.cjq.plan.logical;

import com.cjq.common.FieldFunction;
import com.cjq.exception.EsSqlParseException;
import com.cjq.utils.RequireUtil;

import java.util.List;

import static com.cjq.common.FieldFunction.*;

public class FunctionField extends Field {
    private FieldFunction funcName;
    private Value value;

    public FunctionField(String fieldName, String funcName) {
        super(fieldName);
        setFuncName(funcName);
    }

    public FunctionField(String funcName) {
        setFuncName(funcName);
    }

    public FunctionField(String funcName, Value value) {
        setFuncName(funcName);
        this.value = value;
    }

    public FunctionField() {

    }

    public Value getValue() {
        return value;
    }

    public void setValue(Value value) {
        this.value = value;
    }

    public FieldFunction getFuncName() {
        return funcName;
    }

    public void setFuncName(String funcName) {
        switch (funcName) {
            // agg function
            case "COUNT":
                this.funcName = COUNT;
                this.funcName.setAggFunction(true);
                break;
            case "SUM":
                this.funcName = SUM;
                this.funcName.setAggFunction(true);
                break;
            case "AVG":
                this.funcName = AVG;
                this.funcName.setAggFunction(true);
                break;
            case "MAX":
                this.funcName = MAX;
                this.funcName.setAggFunction(true);
                break;
            case "MIN":
                this.funcName = MIN;
                this.funcName.setAggFunction(true);
                break;
            // function
            case "CONCAT":
                this.funcName = CONCAT;
                break;
            case "CONCAT_WS":
                this.funcName = CONCAT_WS;
                break;
            case "SUBSTRING":
            case "SUBSTR":
                this.funcName = SUBSTRING;
                break;
            case "LENGTH":
                this.funcName = LENGTH;
                break;
            case "UPPER":
                this.funcName = UPPER;
                break;
            case "LOWER":
                this.funcName = LOWER;
                break;
            case "TRIM":
                this.funcName = TRIM;
                break;
            case "REPLACE":
                this.funcName = REPLACE;
                break;
            case "ABS":
                this.funcName = ABS;
                break;
            case "CEIL":
                this.funcName = CEIL;
                break;
            case "FLOOR":
                this.funcName = FLOOR;
                break;
            case "ROUND":
                this.funcName = ROUND;
                break;
            case "MOD":
                this.funcName = MOD;
                break;
            case "POW":
                this.funcName = POW;
                break;
            case "SQRT":
                this.funcName = SQRT;
                break;
            case "RAND":
                this.funcName = RAND;
                break;
            case "NOW":
                this.funcName = NOW;
                break;
            case "CURDATE":
                this.funcName = CURDATE;
                break;
            case "CURTIME":
                this.funcName = CURTIME;
                break;
            case "DATE_FORMAT":
                this.funcName = DATE_FORMAT;
                break;
            case "DATEDIFF":
                this.funcName = DATEDIFF;
                break;
            case "DAY":
                this.funcName = DAY;
                break;
            case "MONTH":
                this.funcName = MONTH;
                break;
            case "YEAR":
                this.funcName = YEAR;
                break;
            case "HOUR":
                this.funcName = HOUR;
                break;
            case "MINUTE":
                this.funcName = MINUTE;
                break;
            case "IF":
                this.funcName = IF;
                break;
            case "IFNULL":
                this.funcName = IFNULL;
                break;
            case "COALESCE":
                this.funcName = COALESCE;
                break;
            case "CASE_WHEN":
                this.funcName = CASE_WHEN;
                break;
            case "CAST":
                this.funcName = CAST;
                break;
            default:
                throw new EsSqlParseException("The function " + funcName + " is not supported.");
        }
    }

    @Override
    public String toString() {
        return "FunctionField{" +
                "funcName=" + funcName +
                ", value=" + value +
                '}';
    }

    public static class MultipleValueFunctionField extends FunctionField {
        private LogicalPlan logicalPlan;
        private List<Value> values;

        public MultipleValueFunctionField(String funcName, LogicalPlan logicalPlan) {
            super(funcName);
            this.logicalPlan = logicalPlan;
        }

        public LogicalPlan getLogicalPlan() {
            return logicalPlan;
        }

        public void setLogicalPlan(LogicalPlan logicalPlan) {
            this.logicalPlan = logicalPlan;
        }

        public List<Value> getValues() {
            return values;
        }

        public void setValues(List<Value> values) {
            switch (super.getFuncName()) {
                case SUBSTRING:
                    if (values.size() != 1 && values.size() != 2) {
                        throw new EsSqlParseException("The function " + super.getFuncName() + " requires one or two arguments.");
                    }
                    values.forEach(v -> RequireUtil.requireTypeCheck(v.getText(), Integer.class));
                    break;
                case REPLACE:
                    if (values.size() != 2) {
                        throw new EsSqlParseException("The function " + super.getFuncName() + " requires two arguments.");
                    }
                    values.forEach(v -> RequireUtil.requireTypeCheck(v.getText(), String.class));
                    break;
                case MOD:
                case POW:
                    if (values.size() != 1) {
                        throw new EsSqlParseException("The function " + super.getFuncName() + " requires one arguments.");
                    }
                    RequireUtil.requireTypeCheck(values.get(0).getText(), Integer.class);
                    break;
                case IF:
                    if (values.size() != 2) {
                        throw new EsSqlParseException("The function " + super.getFuncName() + " requires two arguments.");
                    }
                    break;
                default:
                    throw new EsSqlParseException("The function " + super.getFuncName() + " is not supported.");
            }
            this.values = values;
        }

        @Override
        public String toString() {
            return "MultipValueFunctionField{" +
                    "logicalPlan=" + logicalPlan +
                    ", values=" + values +
                    '}';
        }
    }

    public static class MultipleFieldValueFunctionField extends FunctionField {
        private List<LogicalPlan> multipleLogicalPlan;

        public MultipleFieldValueFunctionField(String funcName) {
            super(funcName);
        }

        public MultipleFieldValueFunctionField(String fieldName, String funcName) {
            super(fieldName, funcName);
        }

        public List<LogicalPlan> getMultipleLogicalPlan() {
            return multipleLogicalPlan;
        }

        public void setMultipleLogicalPlan(List<LogicalPlan> multipleLogicalPlan) {
            if (super.getFuncName().equals(IFNULL)) {
                if (multipleLogicalPlan.size() != 2) {
                    throw new EsSqlParseException("The function " + super.getFuncName() + " requires two arguments.");
                }
            }
            this.multipleLogicalPlan = multipleLogicalPlan;
        }

        @Override
        public String toString() {
            return "MultipleFieldValueFunctionField{" +
                    "multipleLogicalPlan=" + multipleLogicalPlan +
                    '}';
        }
    }

    public static class CaseWhenThenFunctionField extends FunctionField {
        private List<Where> wheres;
        private List<Value> then;
        private Value elseValue;

        public CaseWhenThenFunctionField(String funcName) {
            super(funcName);
        }

        public CaseWhenThenFunctionField(String fieldName, String funcName) {
            super(fieldName, funcName);
        }


        public List<Where> getWheres() {
            return wheres;
        }

        public void setWheres(List<Where> wheres) {
            this.wheres = wheres;
        }

        public List<Value> getThen() {
            return then;
        }

        public void setThen(List<Value> then) {
            this.then = then;
        }

        public Value getElseValue() {
            return elseValue;
        }

        public void setElseValue(Value elseValue) {
            this.elseValue = elseValue;
        }

        @Override
        public String toString() {
            return "CaseWhenThenFunctionField{" +
                    "wheres=" + wheres +
                    ", then=" + then +
                    ", elseValue=" + elseValue +
                    '}';
        }
    }
}
