package com.cjq;

import com.cjq.domain.EqlParserDriver;
import com.cjq.parser.AstBuilder;
import com.cjq.parser.SqlBaseLexer;
import com.cjq.parser.SqlBaseParser;
import com.cjq.plan.logical.LogicalPlan;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.Test;

public class TestDemo {
    @Test
    public void test() {
        String sql = "UPDATE T1 SET F1 = '1', F2='2' WHERE F3 = '3' ";
        SqlBaseLexer lexer = new SqlBaseLexer(new EqlParserDriver.UpperCaseCharStream(CharStreams.fromString(sql)));
        SqlBaseParser parser = new SqlBaseParser(new CommonTokenStream(lexer));
        AstBuilder astBuilder = new AstBuilder();
        LogicalPlan plan = astBuilder.visitSingleStatement(parser.singleStatement());
        System.out.println(plan);
    }
}
