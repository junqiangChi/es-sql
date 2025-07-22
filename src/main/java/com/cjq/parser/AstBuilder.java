package com.cjq.parser;

import com.cjq.common.Constant;
import com.cjq.common.WhereOpr;
import com.cjq.exception.EsSqlParseException;
import com.cjq.exception.ExceptionHandler;
import com.cjq.exception.ErrorCode;
import com.cjq.plan.logical.*;
import org.antlr.v4.runtime.tree.ErrorNodeImpl;
import org.antlr.v4.runtime.tree.ParseTree;

import java.math.BigDecimal;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * AST Builder for SQL parsing, converts parse tree to logical plan
 */
public class AstBuilder extends SqlBaseParserBaseVisitor<LogicalPlan> {

    // Constants for operator symbols
    private static final String NOT_REGEXP = "NOT REGEXP";
    private static final String REGEXP = "REGEXP";
    private static final String NOT_IN = "NOT IN";
    private static final String IN = "IN";
    private static final String NOT_LIKE = "NOT LIKE";
    private static final String LIKE = "LIKE";
    private static final String NOT_ILIKE = "NOT ILIKE";
    private static final String ILIKE = "ILIKE";
    private static final String NOT_BETWEEN = "NOT BETWEEN";
    private static final String BETWEEN = "BETWEEN";

    // Error messages
    private static final String ERROR_SINGLE_STATEMENT = "Single statement error: ";
    private static final String ERROR_FIELD_NAME_INCORRECT = "Field name incorrect, cannot start with a number";
    private static final String ERROR_ONLY_GROUP_BY_SUPPORTED = "Only support group by clause";
    private static final String ERROR_NOT_SUPPORTED_SYNTAX = "not supported syntax: ";
    private static final String ERROR_DUPLICATE_FIELD = "Duplicate field: ";
    private static final String ERROR_WHERE_OR_BY_EMPTY = "WHERE or BY cannot be empty";

    // Operator mapping for predicates
    private static final Map<String, Function<Boolean, String>> OPERATOR_MAPPING = createOperatorMapping();

    private static Map<String, Function<Boolean, String>> createOperatorMapping() {
        Map<String, Function<Boolean, String>> mapping = new HashMap<>();
        mapping.put("RLIKE", hasNot -> hasNot ? NOT_REGEXP : REGEXP);
        mapping.put("IN", hasNot -> hasNot ? NOT_IN : IN);
        mapping.put("LIKE", hasNot -> hasNot ? NOT_LIKE : LIKE);
        mapping.put("ILIKE", hasNot -> hasNot ? NOT_ILIKE : ILIKE);
        mapping.put("BETWEEN", hasNot -> hasNot ? NOT_BETWEEN : BETWEEN);
        return mapping;
    }

    @Override
    public LogicalPlan visitSingleStatement(SqlBaseParser.SingleStatementContext ctx) {
        try {
            validateParseTree(ctx.children);
            return visit(ctx.statement());
        } catch (Exception e) {
            ExceptionHandler.getInstance().handleException(e);
            throw ExceptionHandler.getInstance().createBaseException(e, ErrorCode.SQL_PARSE_ERROR);
        }
    }

    @Override
    public LogicalPlan visitQuery(SqlBaseParser.QueryContext ctx) {
        LogicalPlan query = visit(ctx.queryTerm());
        LogicalPlan queryOrganization = query.optionalMap(ctx, (queryContext, executePlan) -> {
            if (queryContext.queryOrganization() != null) {
                executePlan = visit(queryContext.queryOrganization());
            }
            return executePlan;
        });
        query.setPlan(queryOrganization);
        return query;
    }

    @Override
    public LogicalPlan visitQueryPrimaryDefault(SqlBaseParser.QueryPrimaryDefaultContext ctx) {
        return visit(ctx.querySpecification());
    }

    @Override
    public LogicalPlan visitRegularQuerySpecification(SqlBaseParser.RegularQuerySpecificationContext ctx) {
        SqlBaseParser.SelectClauseContext selectClauseContext = ctx.selectClause();
        From from = (From) visit(ctx.fromClause());
        List<Field> fields = selectClauseContext.namedExpressionSeq().namedExpression().stream().map(this::getField).collect(Collectors.toList());
        SqlBaseParser.WhereClauseContext whereClauseContext = ctx.whereClause();
        Where where = whereClauseContext == null ? null : (Where) visit(whereClauseContext);
        GroupBy groupBy = ctx.aggregationClause() == null ? null : (GroupBy) visit(ctx.aggregationClause());
        return new Query(new Select(fields), from, where, groupBy);
    }

    @Override
    public LogicalPlan visitSearchedCase(SqlBaseParser.SearchedCaseContext ctx) {
        List<SqlBaseParser.WhenClauseContext> whenClauseContexts = ctx.whenClause();
        List<Where> wheres = new ArrayList<>();
        ArrayList<Value> then = new ArrayList<>();
        whenClauseContexts.forEach(whereClauseContext -> {
            List<SqlBaseParser.ExpressionContext> expressions = whereClauseContext.expression();
            wheres.add((Where) visit(expressions.get(0)));
            then.add((Value) visit(expressions.get(1)));
        });
        FunctionField.CaseWhenThenFunctionField caseWhen = new FunctionField.CaseWhenThenFunctionField("CASE_WHEN");
        caseWhen.setWheres(wheres);
        caseWhen.setThen(then);
        caseWhen.setElseValue((Value) visit(ctx.expression()));
        return caseWhen;
    }

    @Override
    public LogicalPlan visitFunctionCall(SqlBaseParser.FunctionCallContext ctx) {
        String funcName = ctx.functionName().getText().toUpperCase();
        LogicalPlan logicalPlan = visit(ctx.functionArgument(0));
        FunctionField functionField;
        switch (funcName) {
            case "CONCAT":
            case "CONCAT_WS":
            case "IFNULL":
            case "COALESCE":
                FunctionField.MultipleFieldValueFunctionField multipleFieldValueFunctionField = new FunctionField.MultipleFieldValueFunctionField(funcName);
                List<LogicalPlan> concatFields = ctx.functionArgument().stream().map(f -> visit(f.expression())).collect(Collectors.toList());
                multipleFieldValueFunctionField.setMultipleLogicalPlan(concatFields);
                functionField = multipleFieldValueFunctionField;
                break;
            case "SUBSTRING":
            case "SUBSTR":
            case "REPLACE":
            case "POW":
            case "MOD":
            case "IF":
                FunctionField.MultipleValueFunctionField multipleValueFunctionField =
                        new FunctionField.MultipleValueFunctionField(funcName, visit(ctx.functionArgument(0)));
                multipleValueFunctionField.setLogicalPlan(visit(ctx.functionArgument(0)));
                List<Value> values = ctx.functionArgument().subList(1, ctx.functionArgument().size()).stream()
                        .map(fArg -> (Value) visit(fArg.expression()))
                        .collect(Collectors.toList());
                multipleValueFunctionField.setValues(values);
                functionField = multipleValueFunctionField;
                break;
            default:
                functionField = logicalPlan instanceof Field ? new FunctionField(((Field) logicalPlan).getFieldName(), funcName) :
                        new FunctionField(funcName, (Value) logicalPlan);
        }

        return functionField;
    }

    @Override
    public LogicalPlan visitColumnReference(SqlBaseParser.ColumnReferenceContext ctx) {
        String fieldName = ctx.getText();
        Field field = new Field(fieldName);
        if (fieldName.contains(Constant.POINT)) {
            field.setNested(true);
        }
        return field;
    }

    /**
     * Process fields
     *
     * @param ctx the parse tree
     * @return LogicalPlan
     */
    private Field getField(SqlBaseParser.NamedExpressionContext ctx) {
        try {
            LogicalPlan visit = visit(ctx.expression().booleanExpression());
            if (visit instanceof Field) {
                Field field = (Field) visit;
                if (ctx.errorCapturingIdentifier() != null) {
                    field.setAlias(ctx.errorCapturingIdentifier().identifier().getText());
                }
                return field;
            } else if (visit instanceof Value) {
                Value value = (Value) visit;
                return new ConstantField(ctx.errorCapturingIdentifier().identifier().getText(), value.getText());
            } else {
                throw new EsSqlParseException(ERROR_FIELD_NAME_INCORRECT);
            }
        } catch (Exception e) {
            ExceptionHandler.getInstance().handleException(e);
            throw ExceptionHandler.getInstance().createBaseException(e, ErrorCode.INVALID_FIELD_NAME);
        }
    }

    @Override
    public LogicalPlan visitDereference(SqlBaseParser.DereferenceContext ctx) {
        SqlBaseParser.PrimaryExpressionContext primaryExpressionContext = ctx.primaryExpression();
        Field field = (Field) visit(primaryExpressionContext);
        if (primaryExpressionContext instanceof SqlBaseParser.DereferenceContext) {
            field = (Field) visit(primaryExpressionContext);
        }
        if (ctx.DOT() != null && ctx.identifier() != null) {
            field.setNested(true);
            field.setFieldName(field.getFieldName() + ctx.DOT() + ctx.identifier().getText());
        }
        return field;
    }

    @Override
    public LogicalPlan visitAggregationClause(SqlBaseParser.AggregationClauseContext ctx) {
        if (ctx.GROUP() != null) {
            List<Field> groupByFields = ctx.groupByClause().stream().map(f -> (Field) visit(f)).collect(Collectors.toList());
            return new GroupBy(groupByFields);
        } else {
            throw new EsSqlParseException(ERROR_ONLY_GROUP_BY_SUPPORTED);
        }
    }

    @Override
    public LogicalPlan visitValueExpressionDefault(SqlBaseParser.ValueExpressionDefaultContext ctx) {
        return visit(ctx.primaryExpression());
    }

    @Override
    public LogicalPlan visitStar(SqlBaseParser.StarContext ctx) {
        return new Field("*");
    }

    /**
     * Process table name and alias
     *
     * @param ctx the parse tree
     * @return LogicalPlan
     */
    @Override
    public LogicalPlan visitTableName(SqlBaseParser.TableNameContext ctx) {
        String tableName = ctx.identifierReference().getText();
        String tableAlias = ctx.tableAlias().getText();
        return new From(tableName, tableAlias);
    }

    @Override
    public LogicalPlan visitNumericLiteral(SqlBaseParser.NumericLiteralContext ctx) {
        return visit(ctx.number());
    }

    @Override
    public LogicalPlan visitIntegerLiteral(SqlBaseParser.IntegerLiteralContext ctx) {
        String text = ctx.getText();
        return text.contains(Constant.POINT) ? new Value(Double.parseDouble(text)) : new Value(Integer.parseInt(text));
    }

    @Override
    public LogicalPlan visitFloatLiteral(SqlBaseParser.FloatLiteralContext ctx) {
        return new Value(Float.parseFloat(ctx.getText()));
    }

    @Override
    public LogicalPlan visitExponentLiteral(SqlBaseParser.ExponentLiteralContext ctx) {
        return new Value(new BigDecimal(ctx.getText()));
    }

    @Override
    public LogicalPlan visitDecimalLiteral(SqlBaseParser.DecimalLiteralContext ctx) {
        return new Value(Double.parseDouble(ctx.getText()));
    }

    @Override
    public LogicalPlan visitDoubleLiteral(SqlBaseParser.DoubleLiteralContext ctx) {
        return new Value(Double.parseDouble(ctx.getText()));
    }

    @Override
    public LogicalPlan visitComparison(SqlBaseParser.ComparisonContext ctx) {
        List<SqlBaseParser.ValueExpressionDefaultContext> expressions = ctx.valueExpression().stream().map(f -> (SqlBaseParser.ValueExpressionDefaultContext) f).collect(Collectors.toList());

        SqlBaseParser.ValueExpressionDefaultContext left = expressions.get(0);
        SqlBaseParser.ValueExpressionDefaultContext right = expressions.get(1);

        Condition condition = new Condition((Field) visit(left.primaryExpression()), ctx.comparisonOperator().getText(), (Value) visit(right.primaryExpression()));
        return new Where(condition, null, null);
    }

    @Override
    public LogicalPlan visitPredicated(SqlBaseParser.PredicatedContext ctx) {
        if (ctx.valueExpression() instanceof SqlBaseParser.ComparisonContext) {
            return visit(ctx.valueExpression());
        }

        if (ctx.valueExpression() instanceof SqlBaseParser.ValueExpressionDefaultContext && ctx.predicate() != null) {
            return buildPredicatedWhere(ctx);
        }

        return visit(ctx.valueExpression());
    }

    private Where buildPredicatedWhere(SqlBaseParser.PredicatedContext ctx) {
        try {
            SqlBaseParser.PredicateContext predicate = ctx.predicate();
            String operatorSymbol = getOperatorSymbol(predicate);
            Value value = buildPredicateValue(predicate);
            Condition condition = new Condition((Field) visit(ctx.valueExpression()), operatorSymbol, value);
            return new Where(condition, null, null);
        } catch (Exception e) {
            ExceptionHandler.getInstance().handleException(e);
            throw ExceptionHandler.getInstance().createBaseException(e, ErrorCode.SQL_PARSE_ERROR);
        }
    }

    private Value buildPredicateValue(SqlBaseParser.PredicateContext predicate) {
        List<SqlBaseParser.ExpressionContext> expressions = predicate.expression();

        if (expressions != null && !expressions.isEmpty()) {
            return buildValueFromExpressions(expressions);
        } else if (predicate.valueExpression() != null && predicate.valueExpression().size() > 1) {
            return buildValueFromValueExpressions(predicate.valueExpression());
        } else {
            return (Value) visit(predicate.valueExpression(0));
        }
    }

    private Value buildValueFromExpressions(List<SqlBaseParser.ExpressionContext> expressions) {
        Value value = null;
        for (SqlBaseParser.ExpressionContext expressionContext : expressions) {
            if (value == null) {
                value = (Value) visit(expressionContext);
            } else {
                value.setPlan(visit(expressionContext));
            }
        }
        return value;
    }

    private Value buildValueFromValueExpressions(List<SqlBaseParser.ValueExpressionContext> valueExpressions) {
        Value value = null;
        for (SqlBaseParser.ValueExpressionContext valueExpressionContext : valueExpressions) {
            if (value == null) {
                value = (Value) visit(valueExpressionContext);
            } else {
                value.setPlan(visit(valueExpressionContext));
            }
        }
        return value;
    }

    @Override
    public LogicalPlan visitLogicalBinary(SqlBaseParser.LogicalBinaryContext ctx) {
        List<SqlBaseParser.BooleanExpressionContext> booleanExpressions = ctx.booleanExpression();
        SqlBaseParser.BooleanExpressionContext left = booleanExpressions.get(0);
        SqlBaseParser.BooleanExpressionContext right = booleanExpressions.get(1);

        WhereOpr whereOpr = WhereOpr.valueOf(ctx.operator.getText().toUpperCase());

        Where where = (Where) visit(right);
        where.setOpr(whereOpr);
        where.setNextWhere((Where) visit(left));

        return where;
    }

    /**
     * When the value after where is a constant, it will return single quotes,
     * so process it in this method to remove the quotes
     *
     * @param ctx the parse tree with {@link SqlBaseParser.StringLiteralContext}
     * @return LogicalPlan
     */
    @Override
    public LogicalPlan visitStringLiteral(SqlBaseParser.StringLiteralContext ctx) {
        String valueStr = ctx.stringLit(0).STRING_LITERAL().getText();
        return new Value(valueStr.substring(1, valueStr.length() - 1));
    }

    @Override
    public LogicalPlan visitSortItem(SqlBaseParser.SortItemContext ctx) {
        String field = ctx.expression().getText();
        Sort.OrderType orderType = ctx.getChildCount() > 1 ? Sort.OrderType.valueOf(ctx.getChild(1).getText().toUpperCase()) : Sort.OrderType.ASC;
        return new Sort(field, orderType);
    }

    @Override
    public LogicalPlan visitQueryOrganization(SqlBaseParser.QueryOrganizationContext ctx) {
        OrderBy orderBy = new OrderBy();
        List<Sort> sorts = ctx.sortItem().stream().map(this::visit).map(sort -> (Sort) sort).collect(Collectors.toList());
        orderBy.setSorts(sorts);

        LogicalPlan limitLogical = orderBy.optionalMap(ctx, (queryContext, limit) -> {
            if (queryContext.LIMIT() != null) {
                limit = buildLimit(queryContext);
            }
            return limit;
        });
        orderBy.setPlan(limitLogical);
        return orderBy;
    }

    private Limit buildLimit(SqlBaseParser.QueryOrganizationContext ctx) {
        if (ctx.limitPagination() != null) {
            List<String> values = ctx.limitPagination().INTEGER_VALUE().stream().map(Object::toString).collect(Collectors.toList());
            return new Limit(Integer.parseInt(values.get(0)), Integer.parseInt(values.get(1)));
        } else {
            return new Limit(0, Integer.parseInt(ctx.limit.getText()));
        }
    }

    @Override
    public LogicalPlan visitDropTable(SqlBaseParser.DropTableContext ctx) {
        String index = ctx.identifierReference().getText();
        Drop drop = new Drop(index);
        if (ctx.IF() != null && ctx.EXISTS() != null) {
            drop.setCheckExists(true);
        }
        return drop;
    }

    @Override
    public LogicalPlan visitDeleteFromTable(SqlBaseParser.DeleteFromTableContext ctx) {
        String index = ctx.identifierReference().getText();
        String tableAlias = ctx.tableAlias() != null ? ctx.tableAlias().getText() : null;
        From from = new From(index, tableAlias);
        Where where = ctx.whereClause() != null ? (Where) visit(ctx.whereClause()) : null;
        return new Delete(from, where);
    }

    @Override
    public LogicalPlan visitShowTables(SqlBaseParser.ShowTablesContext ctx) {
        if (ctx.FROM() != null || ctx.IN() != null) {
            return new Show(ctx.identifierReference().getText());
        }
        if (ctx.LIKE() != null) {
            String patternText = ctx.pattern.getText();
            return new Show(patternText.substring(1, patternText.length() - 1));
        }
        return new Show();
    }

    @Override
    public LogicalPlan visitSingleInsertQuery(SqlBaseParser.SingleInsertQueryContext ctx) {
        Insert valueInsert = (Insert) visit(ctx.query());
        Insert insert = (Insert) visit(ctx.insertInto());
        insert.setValues(valueInsert.getValues());
        return insert;
    }

    @Override
    public LogicalPlan visitInsertIntoTable(SqlBaseParser.InsertIntoTableContext ctx) {
        Insert insert = new Insert();
        From from = new From(ctx.identifierReference().getText());

        if (ctx.identifierList() != null) {
            List<Field> fields = buildInsertFields(ctx.identifierList().identifierSeq().errorCapturingIdentifier(), insert);
            insert.setFields(fields);
        }

        insert.setFrom(from);
        return insert;
    }

    private List<Field> buildInsertFields(List<SqlBaseParser.ErrorCapturingIdentifierContext> identifiers, Insert insert) {
        List<Field> fields = new ArrayList<>();
        for (int i = 0; i < identifiers.size(); i++) {
            String field = identifiers.get(i).identifier().getText();
            if (field.equals(Constant._ID)) {
                insert.setIdPosition(i);
            }
            fields.add(new Field(field));
        }
        return fields;
    }

    @Override
    public LogicalPlan visitQueryTermDefault(SqlBaseParser.QueryTermDefaultContext ctx) {
        return visit(ctx.queryPrimary());
    }

    @Override
    public LogicalPlan visitInlineTable(SqlBaseParser.InlineTableContext ctx) {
        Insert insert = new Insert();
        ctx.expression().forEach(e -> {
            Insert innerInsert = (Insert) visit(e);
            insert.setValues(innerInsert.getValues());
        });
        return insert;
    }

    @Override
    public LogicalPlan visitRowConstructor(SqlBaseParser.RowConstructorContext ctx) {
        List<Value> row = ctx.namedExpression().stream().map(this::visit).map(value -> (Value) value).collect(Collectors.toList());

        Insert insert = new Insert();
        insert.setRow(row);
        return insert;
    }

    @Override
    public LogicalPlan visitNamedExpression(SqlBaseParser.NamedExpressionContext ctx) {
        return visit(ctx.expression().booleanExpression());
    }

    @Override
    public LogicalPlan visitConstantDefault(SqlBaseParser.ConstantDefaultContext ctx) {
        return visit(ctx.constant());
    }

    @Override
    public LogicalPlan visitUpdateTable(SqlBaseParser.UpdateTableContext ctx) {
        String index = ctx.identifierReference().multipartIdentifier().errorCapturingIdentifier.identifier().getText();
        From from = new From(index);

        List<SqlBaseParser.AssignmentContext> assignments = ctx.setClause().assignmentList().assignment();
        List<Field> fields = new ArrayList<>();
        List<Value> values = new ArrayList<>();
        HashSet<String> fieldSet = new HashSet<>();

        assignments.forEach(assignment -> {
            String field = assignment.multipartIdentifier().errorCapturingIdentifier.identifier().getText();
            if (!fieldSet.add(field)) {
                throw new EsSqlParseException(ERROR_DUPLICATE_FIELD + field + " with Set");
            }
            fields.add(new Field(field));
            values.add((Value) visit(assignment.expression().booleanExpression()));
        });

        if (ctx.BY() != null) {
            String docId = ((Value) visit(ctx.valueExpression())).getText().toString();
            return new Update(from, fields, values, docId);
        } else if (ctx.whereClause() != null) {
            return new UpdateByQuery(from, fields, values, (Where) visit(ctx.whereClause()));
        } else {
            throw new EsSqlParseException(ERROR_WHERE_OR_BY_EMPTY);
        }
    }

    /**
     * Validate parse tree for error nodes
     */
    private void validateParseTree(List<ParseTree> children) {
        try {
            for (ParseTree child : children) {
                if (child instanceof ErrorNodeImpl) {
                    throw new EsSqlParseException(ERROR_SINGLE_STATEMENT + child.getText());
                }
            }
        } catch (Exception e) {
            ExceptionHandler.getInstance().handleException(e);
            throw ExceptionHandler.getInstance().createBaseException(e, ErrorCode.SQL_PARSE_ERROR);
        }
    }

    /**
     * Get operator symbol for predicate
     */
    private String getOperatorSymbol(SqlBaseParser.PredicateContext predicate) {
        try {
            for (Map.Entry<String, Function<Boolean, String>> entry : OPERATOR_MAPPING.entrySet()) {
                String operator = entry.getKey();
                Function<Boolean, String> symbolFunction = entry.getValue();

                if (isPredicateOperator(predicate, operator)) {
                    boolean hasNot = predicate.NOT() != null;
                    return symbolFunction.apply(hasNot);
                }
            }
            throw new EsSqlParseException(ERROR_NOT_SUPPORTED_SYNTAX + predicate.getText());
        } catch (Exception e) {
            ExceptionHandler.getInstance().handleException(e);
            throw ExceptionHandler.getInstance().createBaseException(e, ErrorCode.SQL_SYNTAX_ERROR);
        }
    }

    private boolean isPredicateOperator(SqlBaseParser.PredicateContext predicate, String operator) {
        switch (operator) {
            case "RLIKE":
                return predicate.RLIKE() != null;
            case "IN":
                return predicate.IN() != null;
            case "LIKE":
                return predicate.LIKE() != null;
            case "ILIKE":
                return predicate.ILIKE() != null;
            case "BETWEEN":
                return predicate.BETWEEN() != null && predicate.AND() != null;
            default:
                return false;
        }
    }
}
