package com.cjq;

import com.cjq.jdbc.EsDriver;
import com.cjq.parser.AstBuilder;
import com.cjq.parser.SqlBaseLexer;
import com.cjq.parser.SqlBaseParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.Test;

public class TestDemo {
    @Test
    public void test(){
        String sql = "select f1,F2,fff, FFff from tbl1 where f1 match 'a'";
        SqlBaseLexer lexer = new SqlBaseLexer(new EsDriver.UpperCaseCharStream(CharStreams.fromString(sql)));
        SqlBaseParser parser = new SqlBaseParser(new CommonTokenStream(lexer));
        AstBuilder astBuilder = new AstBuilder();
        astBuilder.visitQuery(parser.query());

    }
}
