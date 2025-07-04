package com.cjq.parser;

import com.cjq.common.Constant;
import com.cjq.common.WhereOpr;
import com.cjq.exception.EsSqlParseException;
import com.cjq.plan.logical.*;
import org.antlr.v4.runtime.tree.ErrorNodeImpl;
import org.antlr.v4.runtime.tree.ParseTree;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class AstBuilder extends SqlBaseParserBaseVisitor<LogicalPlan> {
    @Override
    public LogicalPlan visitSingleStatement(SqlBaseParser.SingleStatementContext ctx) {
        List<ParseTree> children = ctx.children;
        for (ParseTree child : children) {
            if (child instanceof ErrorNodeImpl) {
                throw new EsSqlParseException("Single statement error: " + child.getText());
            }
        }
        return visit(ctx.statement());
    }

    @Override
    public LogicalPlan visitQuery(SqlBaseParser.QueryContext ctx) {
        LogicalPlan query = visit(ctx.queryTerm());
        // 参考spark源码
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
        List<Field> fields = selectClauseContext
                .namedExpressionSeq()
                .namedExpression()
                .stream().map(this::getField)
                .collect(Collectors.toList());
        SqlBaseParser.WhereClauseContext whereClauseContext = ctx.whereClause();
        Where where = whereClauseContext == null ? null : (Where) visit(whereClauseContext);
        GroupBy groupBy = ctx.aggregationClause() == null ? null : (GroupBy) visit(ctx.aggregationClause());
        return new Query(new Select(fields), from, where, groupBy);
    }

    @Override
    public LogicalPlan visitFunctionCall(SqlBaseParser.FunctionCallContext ctx) {
        String funcName = ctx.functionName().getText().toUpperCase();
        String fieldName = ctx.functionArgument(0).getText();
        return new Field(fieldName, funcName);
    }

    @Override
    public LogicalPlan visitColumnReference(SqlBaseParser.ColumnReferenceContext ctx) {
        return new Field(ctx.getText());
    }

    private Field getField(SqlBaseParser.NamedExpressionContext ctx) {
        LogicalPlan visit = visit(ctx.expression().booleanExpression());
        if (visit instanceof Value) {
            throw new EsSqlParseException("Field name incorrect, cannot start with a number");
        }
        Field field = (Field) visit;
        if (ctx.errorCapturingIdentifier() != null) {
            field.setAlias(ctx.errorCapturingIdentifier().getText());
        }
        return field;
    }

    @Override
    public LogicalPlan visitAggregationClause(SqlBaseParser.AggregationClauseContext ctx) {
        if (ctx.GROUP() != null) {
            List<SqlBaseParser.GroupByClauseContext> groupByClauseContexts = ctx.groupByClause();
            List<Field> groupByField = groupByClauseContexts.stream().map(f -> (Field) visit(f))
                    .collect(Collectors.toList());
            return new GroupBy(groupByField);
        } else {
            throw new EsSqlParseException("Only support group by clause");
        }
    }

    @Override
    public LogicalPlan visitValueExpressionDefault(SqlBaseParser.ValueExpressionDefaultContext ctx) {
        if (ctx.primaryExpression() instanceof SqlBaseParser.ConstantDefaultContext) {
            return visit(((SqlBaseParser.ConstantDefaultContext) ctx.primaryExpression()).constant());
        } else if (ctx.primaryExpression() instanceof SqlBaseParser.RowConstructorContext) {
            return visit(ctx.primaryExpression());
        } else if (ctx.primaryExpression() instanceof SqlBaseParser.ColumnReferenceContext) {
            return visit(ctx.primaryExpression());
        } else {
            return visit(ctx.primaryExpression());
        }
    }

    @Override
    public LogicalPlan visitStar(SqlBaseParser.StarContext ctx) {
        return new Field("*");
    }

    /**
     * 处理表名及别名
     *
     * @param ctx the parse tree
     * @return
     */
    @Override
    public LogicalPlan visitTableName(SqlBaseParser.TableNameContext ctx) {
        String tableName = ctx.identifierReference().getText();
        String tableAlias = ctx.tableAlias().getText();
        return new From(tableName, tableAlias);
    }

    @Override
    public LogicalPlan visitNumericLiteral(SqlBaseParser.NumericLiteralContext ctx) {
        if (ctx.number() instanceof SqlBaseParser.IntegerLiteralContext) {
            return visitIntegerLiteral((SqlBaseParser.IntegerLiteralContext) ctx.number());
        } else if (ctx.number() instanceof SqlBaseParser.DecimalLiteralContext) {
            return visitDecimalLiteral((SqlBaseParser.DecimalLiteralContext) ctx.number());
        } else if (ctx.number() instanceof SqlBaseParser.FloatLiteralContext) {
            return visitFloatLiteral((SqlBaseParser.FloatLiteralContext) ctx.number());
        } else if (ctx.number() instanceof SqlBaseParser.ExponentLiteralContext) {
            return visitExponentLiteral((SqlBaseParser.ExponentLiteralContext) ctx.number());
        } else {
            return visitDoubleLiteral((SqlBaseParser.DoubleLiteralContext) ctx.number());
        }
    }

    @Override
    public LogicalPlan visitIntegerLiteral(SqlBaseParser.IntegerLiteralContext ctx) {
        if (ctx.getText().contains(".")) {
            return new Value(Double.parseDouble(ctx.getText()));
        } else {
            return new Value(Integer.parseInt(ctx.getText()));
        }
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
        List<SqlBaseParser.ValueExpressionDefaultContext> valueExpressionDefaultContexts = ctx.valueExpression()
                .stream().map(f -> (SqlBaseParser.ValueExpressionDefaultContext) f).collect(Collectors.toList());
        SqlBaseParser.ValueExpressionDefaultContext left = valueExpressionDefaultContexts.get(0);
        SqlBaseParser.ValueExpressionDefaultContext right = valueExpressionDefaultContexts.get(1);
        Condition condition = new Condition(left.primaryExpression().getText(), ctx.comparisonOperator().getText(),
                (Value) visit(right.primaryExpression()));
        return new Where(condition, null, null);
    }

    @Override
    public LogicalPlan visitPredicated(SqlBaseParser.PredicatedContext ctx) {
        if (ctx.valueExpression() instanceof SqlBaseParser.ComparisonContext) {
            return visit(ctx.valueExpression());
        }
        if (ctx.valueExpression() instanceof SqlBaseParser.ValueExpressionDefaultContext && ctx.predicate() != null) {
            SqlBaseParser.PredicateContext predicate = ctx.predicate();
            String operatorSymbol = getOperatorSymbol(predicate);
            Value value = null;
            List<SqlBaseParser.ExpressionContext> expression = predicate.expression();
            if (expression != null && !expression.isEmpty()) {
                for (SqlBaseParser.ExpressionContext expressionContext : expression) {
                    if (value == null) {
                        value = (Value) visit(expressionContext);
                    } else {
                        value.setPlan(visit(expressionContext));
                    }
                }
            } else if (predicate.valueExpression() != null && predicate.valueExpression().size() > 1) {
                for (SqlBaseParser.ValueExpressionContext valueExpressionContext : predicate.valueExpression()) {
                    if (value == null) {
                        value = (Value) visit(valueExpressionContext);
                    } else {
                        value.setPlan(visit(valueExpressionContext));
                    }
                }
            } else {
                value = (Value) visit(predicate.valueExpression(0));
            }
            Condition condition = new Condition(ctx.valueExpression().getText(), operatorSymbol, value);
            return new Where(condition, null, null);
        }
        return visit(ctx.valueExpression());
    }


    @Override
    public LogicalPlan visitLogicalBinary(SqlBaseParser.LogicalBinaryContext ctx) {
        List<SqlBaseParser.BooleanExpressionContext> booleanExpressionContexts = ctx.booleanExpression();
        Where where = null;
        SqlBaseParser.BooleanExpressionContext left = booleanExpressionContexts.get(0);
        SqlBaseParser.BooleanExpressionContext right = booleanExpressionContexts.get(1);
        WhereOpr whereOpr = WhereOpr.valueOf(ctx.operator.getText().toUpperCase());
        if (right instanceof SqlBaseParser.PredicatedContext) {
            where = (Where) visit(right);
            where.setOpr(whereOpr);
        }
        if (left instanceof SqlBaseParser.LogicalBinaryContext) {
            where.setNextWhere((Where) visit(left));
        } else {
            where.setNextWhere((Where) visit(left));
        }
        return where;
    }

    /**
     * 当where的后面为常量时，会返回单引号，因此在这个方法中做处理,去除单引号
     *
     * @param ctx the parse tree with {@link SqlBaseParser.StringLiteralContext}
     * @return {@link LogicalPlan}
     */
    @Override
    public LogicalPlan visitStringLiteral(SqlBaseParser.StringLiteralContext ctx) {
        String valueStr = ctx.stringLit(0).STRING_LITERAL().getText();
        return new Value(valueStr.substring(1, valueStr.length() - 1));
    }

    @Override
    public LogicalPlan visitSortItem(SqlBaseParser.SortItemContext ctx) {
        return new Sort(ctx.expression().getText(), ctx.getChildCount() > 1 ? Sort.OrderType.valueOf(ctx.getChild(1).getText().toUpperCase()) : Sort.OrderType.ASC);
    }

    @Override
    public LogicalPlan visitQueryOrganization(SqlBaseParser.QueryOrganizationContext ctx) {
        OrderBy orderBy = new OrderBy();
        List<Sort> sorts = ctx.sortItem().stream().map(o -> (Sort) visit(o)).collect(Collectors.toList());
        orderBy.setSorts(sorts);
        LogicalPlan limitLogical = orderBy.optionalMap(ctx, (queryContext, limit) -> {
            if (queryContext.LIMIT() != null) {
                if (queryContext.limitPagination() != null) {
                    limit = new Limit(Integer.parseInt(queryContext.limitPagination().INTEGER_VALUE().get(0).getText()),
                            Integer.parseInt(queryContext.limitPagination().INTEGER_VALUE().get(1).getText()));
                } else {
                    limit = new Limit(0, Integer.parseInt(queryContext.limit.getText()));
                }
            }
            return limit;
        });
        orderBy.setPlan(limitLogical);
        return orderBy;
    }

    /**
     * 判断这个字段是否为常量
     *
     * @param ctx {@link SqlBaseParser.NamedExpressionContext}
     * @return {@link LogicalPlan}
     */
    private boolean isConstant(SqlBaseParser.NamedExpressionContext ctx) {
        SqlBaseParser.BooleanExpressionContext booleanExpression = ctx.expression().booleanExpression();
        if (booleanExpression instanceof SqlBaseParser.PredicatedContext) {
            SqlBaseParser.PredicatedContext predicatedContext = (SqlBaseParser.PredicatedContext) booleanExpression;
            SqlBaseParser.ValueExpressionContext valueExpression = predicatedContext.valueExpression();
            if (valueExpression instanceof SqlBaseParser.ValueExpressionDefaultContext) {
                SqlBaseParser.ValueExpressionDefaultContext valueExpressionDefaultContext = (SqlBaseParser.ValueExpressionDefaultContext) valueExpression;
                SqlBaseParser.PrimaryExpressionContext primaryExpression = valueExpressionDefaultContext.primaryExpression();
                return primaryExpression instanceof SqlBaseParser.ConstantDefaultContext;
            }
        }
        return false;
    }

    private String getOperatorSymbol(SqlBaseParser.PredicateContext predicate) {
        if (predicate.RLIKE() != null) {
            if (predicate.NOT() != null) {
                return "NOT REGEXP";
            } else {
                return "REGEXP";
            }
        } else if (predicate.IN() != null) {
            if (predicate.NOT() != null) {
                return "NOT IN";
            } else {
                return "IN";
            }
        } else if (predicate.LIKE() != null) {
            if (predicate.NOT() != null) {
                return "NOT LIKE";
            } else {
                return "LIKE";
            }
        } else if (predicate.ILIKE() != null) {
            if (predicate.NOT() != null) {
                return "NOT ILIKE";
            } else {
                return "ILIKE";
            }
        } else if (predicate.BETWEEN() != null && predicate.AND() != null) {
            if (predicate.NOT() != null) {
                return "NOT BETWEEN";
            } else {
                return "BETWEEN";
            }
        } else {
            throw new EsSqlParseException("not supported syntax：" + predicate.getText());
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
            List<SqlBaseParser.ErrorCapturingIdentifierContext> errorCapturingIdentifierContexts = ctx.identifierList().identifierSeq().errorCapturingIdentifier();
            List<Field> fields = new ArrayList<>();
            for (int i = 0; i < errorCapturingIdentifierContexts.size(); i++) {
                String field = errorCapturingIdentifierContexts.get(i).identifier().getText();
                if (field.equals(Constant._ID)) {
                    insert.setIdPosition(i);
                }
                fields.add(new Field(field));
            }
            insert.setFields(fields);
        }
        insert.setFrom(from);
        return insert;
    }

    @Override
    public LogicalPlan visitQueryTermDefault(SqlBaseParser.QueryTermDefaultContext ctx) {
        return visit(ctx.queryPrimary());
    }

    @Override
    public LogicalPlan visitInlineTable(SqlBaseParser.InlineTableContext ctx) {
        Insert insert = new Insert();
        ctx.expression()
                .forEach(e -> {
                    Insert innerInsert = (Insert) visit(e);
                    insert.setValues(innerInsert.getValues());
                });
        return insert;
    }

    @Override
    public LogicalPlan visitRowConstructor(SqlBaseParser.RowConstructorContext ctx) {
        List<Value> row = ctx.namedExpression().stream()
                .map(v -> {
                    LogicalPlan visit = visit(v);
                    return (Value) visit;
                })
                .collect(Collectors.toList());
        Insert insert = new Insert();
        insert.setRow(row);
        return insert;
    }

    @Override
    public LogicalPlan visitNamedExpression(SqlBaseParser.NamedExpressionContext ctx) {
        SqlBaseParser.BooleanExpressionContext tree = ctx.expression().booleanExpression();
        return visit(tree);
    }

    @Override
    public LogicalPlan visitConstantDefault(SqlBaseParser.ConstantDefaultContext ctx) {
        return new Value(visit(ctx.constant()));
    }
}
