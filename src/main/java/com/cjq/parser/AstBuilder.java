package com.cjq.parser;

import com.cjq.common.WhereOpr;
import com.cjq.plan.logical.*;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.List;
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
    public LogicalPlan visitRegularQuerySpecification(SqlBaseParser.RegularQuerySpecificationContext ctx) {
        SqlBaseParser.SelectClauseContext selectClauseContext = ctx.selectClause();
        SqlBaseParser.FromClauseContext fromClauseContext = ctx.fromClause();
        String index = fromClauseContext.relation().get(0).getText();
        List<Field> fields = selectClauseContext.namedExpressionSeq().namedExpression().stream().map(f ->
                f.errorCapturingIdentifier() != null ?
                        new Field(f.expression().getText(), f.errorCapturingIdentifier().getText()) :
                        new Field(f.expression().getText())
        ).collect(Collectors.toList());
        SqlBaseParser.WhereClauseContext whereClauseContext = ctx.whereClause();
        Where where = new Where();
        if (whereClauseContext != null) {
            ParseTree parseTree = whereClauseContext.children.get(1);
            for (int i = 0; i < parseTree.getChildCount(); i += 2) {
                System.out.println(parseTree.getChild(i).getText());
                if (where.getCondition() == null) {
                    handleWhere(where, parseTree, i);
                } else {
                    Where nextWhere = new Where();
                    handleWhere(nextWhere, parseTree, i);
                    where.setNextWhere(nextWhere);
                }
            }
        }
        return new Query(new Select(fields), new From(index), where);
    }

    private void handleWhere(Where where, ParseTree parseTree, int i) {
        Where.Condition condition = new Where.Condition(parseTree.getChild(i).getChild(0).getChild(0).getText(), parseTree.getChild(i).getChild(0).getChild(1).getText(), parseTree.getChild(i).getChild(0).getChild(2).getText());
        where.setCondition(condition);
        if (i < parseTree.getChildCount() - 1) {
            where.setOpr(WhereOpr.valueOf(parseTree.getChild(i + 1).getText()));
        }
    }

    @Override
    public LogicalPlan visitQueryOrganization(SqlBaseParser.QueryOrganizationContext ctx) {
        return ctx.limit != null ? new Limit(Integer.parseInt(ctx.limit.getText())) : visit(ctx);
    }

}
