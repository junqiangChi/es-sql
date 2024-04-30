package com.cjq.parser;

import com.cjq.common.OperatorSymbol;
import com.cjq.common.WhereOpr;
import com.cjq.exception.EsSqlParseException;
import com.cjq.plan.logical.*;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.List;
import java.util.Spliterator;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class AstBuilder extends SqlBaseParserBaseVisitor<LogicalPlan> {
    @Override
    public LogicalPlan visitSingleStatement(SqlBaseParser.SingleStatementContext ctx) {
        return visit(ctx.statement());
    }

    @Override
    public LogicalPlan visitQuery(SqlBaseParser.QueryContext ctx) {
        LogicalPlan query = visit(ctx.queryTerm());
        // 参考spark源码
        LogicalPlan limit = query.optionalMap(ctx, (queryContext, executePlan) -> {
            if (!queryContext.queryOrganization().expression().isEmpty()) {
                executePlan = visit(queryContext.queryOrganization());
            }
            return executePlan;
        });
        query.setPlan(limit);
        return query;
    }


    @Override
    public LogicalPlan visitQueryTermDefault(SqlBaseParser.QueryTermDefaultContext ctx) {
        return visit(ctx.queryPrimary());
    }

    @Override
    public LogicalPlan visitQueryPrimaryDefault(SqlBaseParser.QueryPrimaryDefaultContext ctx) {
        return visit(ctx.querySpecification());
    }

    @Override
    public LogicalPlan visitIntervalLiteral(SqlBaseParser.IntervalLiteralContext ctx) {
        return visit(ctx);
    }

    @Override
    public LogicalPlan visitRegularQuerySpecification(SqlBaseParser.RegularQuerySpecificationContext ctx) {
        SqlBaseParser.SelectClauseContext selectClauseContext = ctx.selectClause();
        List<SqlBaseParser.RelationContext> relation = ctx.fromClause().relation();
        From from = new From(relation.get(0).getText());
        if (relation.size() > 1) {
            from.setAlias(relation.get(1).getText());
        }
        List<Field> fields = selectClauseContext.namedExpressionSeq().namedExpression().stream()
                .map(f -> f.errorCapturingIdentifier() != null ? new Field(f.expression().getText(),
                        f.errorCapturingIdentifier().getText(), isConstant(f)) :
                        new Field(f.expression().getText(), null, isConstant(f))).collect(Collectors.toList());
        SqlBaseParser.WhereClauseContext whereClauseContext = ctx.whereClause();
        LogicalPlan where = visit(whereClauseContext);
        return new Query(new Select(fields), from, (Where) where);
    }

    @Override
    public LogicalPlan visitNumericLiteral(SqlBaseParser.NumericLiteralContext ctx) {
        return new Value(ctx.getText());
    }

    @Override
    public LogicalPlan visitComparison(SqlBaseParser.ComparisonContext ctx) {
        List<SqlBaseParser.ValueExpressionDefaultContext> valueExpressionDefaultContexts =
                ctx.valueExpression().stream()
                        .map(f -> (SqlBaseParser.ValueExpressionDefaultContext) f).collect(Collectors.toList());
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
            if (predicate.valueExpression() != null && predicate.valueExpression().size() > 1) {
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
        WhereOpr whereOpr = WhereOpr.valueOf(ctx.operator.getText());
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
    public LogicalPlan visitStringLit(SqlBaseParser.StringLitContext ctx) {
        return visit(ctx.STRING_LITERAL());
    }


    @Override
    public LogicalPlan visitQueryOrganization(SqlBaseParser.QueryOrganizationContext ctx) {
        return ctx.limit != null ? new Limit(Integer.parseInt(ctx.limit.getText())) : visit(ctx);
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
                SqlBaseParser.ValueExpressionDefaultContext valueExpressionDefaultContext =
                        (SqlBaseParser.ValueExpressionDefaultContext) valueExpression;
                SqlBaseParser.PrimaryExpressionContext primaryExpression =
                        valueExpressionDefaultContext.primaryExpression();
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
            throw new EsSqlParseException("not supported syntax： " + predicate.getText());
        }
    }

    /**
     * v1.0 使用循环的方式收集wheres
     * 有点蠢，自己一层层往下解析的，应该重写对应的方法，来一层层提取
     * 过时
     *
     * @param logicalBinaryContext A parent ParserContext is {@link SqlBaseParser.LogicalBinaryContext}
     * @param where                A {@link Where} in {@link Query}
     * @return A where {@link Where} in {@link Query}
     */
    @Deprecated
    private Where setWhere(SqlBaseParser.LogicalBinaryContext logicalBinaryContext, Where where) {
        SqlBaseParser.BooleanExpressionContext left = logicalBinaryContext.left;
        SqlBaseParser.BooleanExpressionContext right = logicalBinaryContext.right;
        Token operator = logicalBinaryContext.operator;
        while (left instanceof SqlBaseParser.LogicalBinaryContext) {
            if (where.getOpr() == null) {
                Condition condition = getCondition((SqlBaseParser.PredicatedContext) right);
                where.setCondition(condition);
                where.setOpr(WhereOpr.valueOf(operator.getText()));
            } else {
                Where nextWhere = new Where();
                Condition condition = getCondition((SqlBaseParser.PredicatedContext) right);
                nextWhere.setCondition(condition);
                nextWhere.setOpr(WhereOpr.valueOf(operator.getText()));
                where.setNextWhere(nextWhere);
            }
            right = ((SqlBaseParser.LogicalBinaryContext) left).right;
            operator = ((SqlBaseParser.LogicalBinaryContext) left).operator;
            left = ((SqlBaseParser.LogicalBinaryContext) left).left;
        }
        Where leftWhere = new Where(getCondition((SqlBaseParser.PredicatedContext) left),
                WhereOpr.valueOf(operator.getText()), null);
        Where rightWhere = new Where(getCondition((SqlBaseParser.PredicatedContext) right),
                WhereOpr.valueOf(operator.getText()), leftWhere);
        where.setNextWhere(rightWhere);
        return where;
    }

    /**
     * v1.1 使用递归的方式收集wheres
     * 把v1.0的循环改成了递归，有点聪明，但不多
     * 过时
     *
     * @param logicalBinaryContext A parent ParserContext {@link SqlBaseParser.LogicalBinaryContext}
     * @param where                A {@link Where} in {@link Query}
     * @return A where {@link Where} in {@link Query}
     */
    @Deprecated
    private Where setWhereWithRecursive(SqlBaseParser.LogicalBinaryContext logicalBinaryContext, Where where) {
        SqlBaseParser.BooleanExpressionContext left = logicalBinaryContext.left;
        SqlBaseParser.BooleanExpressionContext right = logicalBinaryContext.right;
        Token operator = logicalBinaryContext.operator;
        if (where.getOpr() == null) {
            Condition condition = getCondition((SqlBaseParser.PredicatedContext) right);
            where.setCondition(condition);
            where.setOpr(WhereOpr.valueOf(operator.getText()));
        } else {
            Where nextWhere = new Where();
            Condition condition = getCondition((SqlBaseParser.PredicatedContext) right);
            nextWhere.setCondition(condition);
            nextWhere.setOpr(WhereOpr.valueOf(operator.getText()));
            where.setNextWhere(nextWhere);
        }
        if (left instanceof SqlBaseParser.LogicalBinaryContext) {
            return setWhereWithRecursive((SqlBaseParser.LogicalBinaryContext) left, where);
        }
        Where leftWhere = new Where(getCondition((SqlBaseParser.PredicatedContext) left),
                WhereOpr.valueOf(operator.getText()), null);
        Where rightWhere = new Where(getCondition((SqlBaseParser.PredicatedContext) right),
                WhereOpr.valueOf(operator.getText()), leftWhere);
        where.setNextWhere(rightWhere);
        return where;
    }

    /**
     * v1.0 笨B写法，自己解析的
     *
     * @param predicatedContext
     * @return
     */
    @Deprecated
    private Condition getCondition(SqlBaseParser.PredicatedContext predicatedContext) {
        SqlBaseParser.ValueExpressionDefaultContext childLeft =
                (SqlBaseParser.ValueExpressionDefaultContext) predicatedContext.valueExpression().getChild(0);
        ParseTree childMid = predicatedContext.valueExpression().getChild(1);
        SqlBaseParser.ValueExpressionDefaultContext childRight =
                (SqlBaseParser.ValueExpressionDefaultContext) predicatedContext.valueExpression().getChild(2);
        return new Condition(childLeft.primaryExpression().getText(), childMid.getText(), new Value());
    }


}
