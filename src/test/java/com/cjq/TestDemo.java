package com.cjq;

import com.cjq.domain.EqlParserDriver;
import com.cjq.parser.AstBuilder;
import com.cjq.parser.SqlBaseLexer;
import com.cjq.parser.SqlBaseParser;
import com.cjq.plan.logical.FunctionField;
import com.cjq.plan.logical.LogicalPlan;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class TestDemo {
    @Test
    public void test() {
//        String sql = "SELECT CASE WHEN F1 ='11' THEN 1  WHEN F1 ='111' THEN 11  ELSE 2 END fff ,f23 f3 FROM T1 WHERE F1 = '1'";
//        String sql = "SELECT CASE WHEN F1 ='11' THEN 1  WHEN F1 ='111' THEN 11  ELSE 2 END fff ,f23 f3, 1 as f4, '22' as f5 FROM T1 WHERE F1 = '1'";
        String sql = "select  f5.f51.f43.f44 from my_index";
        SqlBaseLexer lexer = new SqlBaseLexer(new EqlParserDriver.UpperCaseCharStream(CharStreams.fromString(sql)));
        SqlBaseParser parser = new SqlBaseParser(new CommonTokenStream(lexer));
        AstBuilder astBuilder = new AstBuilder();
        LogicalPlan plan = astBuilder.visitSingleStatement(parser.singleStatement());
        System.out.println(plan);
    }
}
