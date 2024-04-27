package com.cjq.domain;

import com.cjq.parser.AstBuilder;
import com.cjq.parser.SqlBaseLexer;
import com.cjq.parser.SqlBaseParser;
import com.cjq.plan.logical.LogicalPlan;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.Interval;

public class EqlParserDriver {

    public LogicalPlan parser(String sql){
        SqlBaseLexer lexer = new SqlBaseLexer(new UpperCaseCharStream(CharStreams.fromString(sql)));
        lexer.removeErrorListeners();
        lexer.addErrorListener(new ParseErrorListener());
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        SqlBaseParser parser = new SqlBaseParser(tokenStream);

        AstBuilder astBuilder = new AstBuilder();
        return astBuilder.visitSingleStatement(parser.singleStatement());
    }

    public static class UpperCaseCharStream implements CharStream {

        private CodePointCharStream wrapped;

        public UpperCaseCharStream(CodePointCharStream wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public String getText(Interval interval) {
            return wrapped.getText(interval);
        }

        @Override
        public void consume() {
            wrapped.consume();
        }

        @Override
        public int LA(int i) {
            int la = wrapped.LA(i);
            if (la == 0 || la == IntStream.EOF) {
                return la;
            }
            return Character.toUpperCase(la);
        }

        @Override
        public int mark() {
            return wrapped.mark();
        }

        @Override
        public void release(int marker) {
            wrapped.release(marker);
        }

        @Override
        public int index() {
            return wrapped.index();
        }

        @Override
        public void seek(int index) {

        }

        @Override
        public int size() {
            return wrapped.size();
        }

        @Override
        public String getSourceName() {
            return wrapped.getSourceName();
        }
    }
    public static class ParseErrorListener extends BaseErrorListener {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine,
                                String msg, RecognitionException e) {
            if (offendingSymbol instanceof CommonToken) {
                // TODO
            }
        }
    }
}
