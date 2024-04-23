package com.cjq.parser;

import com.cjq.plan.logical.LogicalPlan;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.List;

public class AstBuilder extends SqlBaseParserBaseVisitor<LogicalPlan> {
    @Override
    public LogicalPlan visitSingleStatement(SqlBaseParser.SingleStatementContext ctx) {
        return super.visitSingleStatement(ctx);
    }

    @Override
    public LogicalPlan visitSingleTableIdentifier(SqlBaseParser.SingleTableIdentifierContext ctx) {
        return super.visitSingleTableIdentifier(ctx);
    }

    @Override
    public LogicalPlan visitUse(SqlBaseParser.UseContext ctx) {
        return super.visitUse(ctx);
    }

    @Override
    public LogicalPlan visitRegularQuerySpecification(SqlBaseParser.RegularQuerySpecificationContext ctx) {
        SqlBaseParser.SelectClauseContext selectClauseContext = ctx.selectClause();
        List<SqlBaseParser.NamedExpressionContext> namedExpressionContexts = selectClauseContext.namedExpressionSeq().namedExpression();
        for (SqlBaseParser.NamedExpressionContext namedExpressionContext : namedExpressionContexts) {
            System.out.println(namedExpressionContext.expression().getText());
        }
        return null;
    }
}
