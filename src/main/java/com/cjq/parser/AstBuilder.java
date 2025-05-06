package com.cjq.parser;

import com.cjq.common.WhereOpr;
import com.cjq.exception.EsSqlParseException;
import com.cjq.plan.logical.*;
import org.antlr.v4.runtime.tree.ErrorNodeImpl;
import org.antlr.v4.runtime.tree.ParseTree;

import java.math.BigDecimal;
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
        List<Field> fields = selectClauseContext.namedExpressionSeq().namedExpression().stream().map(f -> {
            if (f.errorCapturingIdentifier() != null) {
                if (isConstant(f)) {
                    return new Field(null, f.errorCapturingIdentifier().getText(), true, ((Value) visit(f.expression())).getText());
                } else {
                    return new Field(f.expression().getText(), f.errorCapturingIdentifier().getText(), false, null);
                }
            } else {
                return new Field(f.expression().getText(), null, false, null);
            }
        }).collect(Collectors.toList());
        SqlBaseParser.WhereClauseContext whereClauseContext = ctx.whereClause();
        LogicalPlan where = whereClauseContext == null ? null : visit(whereClauseContext);
        return new Query(new Select(fields), from, (Where) where);
    }

    @Override
    public LogicalPlan visitValueExpressionDefault(SqlBaseParser.ValueExpressionDefaultContext ctx) {
        if (ctx.primaryExpression() instanceof SqlBaseParser.ColumnReferenceContext) {
            return visit(ctx.primaryExpression());
        } else if (ctx.primaryExpression() instanceof SqlBaseParser.ConstantDefaultContext) {
            SqlBaseParser.ConstantDefaultContext constantDefaultContext = (SqlBaseParser.ConstantDefaultContext) ctx.primaryExpression();
            if (constantDefaultContext.constant() instanceof SqlBaseParser.StringLiteralContext) {
                return visitStringLiteral((SqlBaseParser.StringLiteralContext) constantDefaultContext.constant());
            } else if (constantDefaultContext.constant() instanceof SqlBaseParser.NumericLiteralContext) {
                return visitNumericLiteral((SqlBaseParser.NumericLiteralContext) constantDefaultContext.constant());
            }
        }
        return visit(ctx);
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
        List<LogicalPlan> sorts = ctx.sortItem().stream().map(this::visit).collect(Collectors.toList());
        orderBy.setSorts(sorts);
        LogicalPlan limit = ctx.limit != null ? new Limit(Integer.parseInt(ctx.limit.getText())) : null;
        orderBy.setPlan(limit);
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
}
