package com.cjq.domain;

import com.cjq.parser.AstBuilder;
import com.cjq.parser.SqlBaseLexer;
import com.cjq.parser.SqlBaseParser;
import com.cjq.plan.logical.LogicalPlan;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.Interval;

/**
 * SQL Parser Driver for Elasticsearch
 * Provides the main entry point for parsing SQL statements into logical plans
 * Uses ANTLR4 for lexical analysis and parsing, with custom error handling
 */
public class EqlParserDriver {

    /**
     * Parses a SQL statement into a logical plan
     * Creates a lexer and parser with custom error handling, then builds the AST
     * 
     * @param sql the SQL statement to parse
     * @return the logical plan representing the parsed SQL
     */
    public LogicalPlan parser(String sql){
        // Create lexer with case-insensitive input stream
        SqlBaseLexer lexer = new SqlBaseLexer(new UpperCaseCharStream(CharStreams.fromString(sql)));
        lexer.removeErrorListeners();
        lexer.addErrorListener(new ParseErrorListener());
        
        // Create parser with token stream
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        SqlBaseParser parser = new SqlBaseParser(tokenStream);

        // Build AST using AstBuilder
        AstBuilder astBuilder = new AstBuilder();
        return astBuilder.visitSingleStatement(parser.singleStatement());
    }

    /**
     * Custom character stream that converts input to uppercase
     * Ensures case-insensitive SQL parsing
     */
    public static class UpperCaseCharStream implements CharStream {

        /** The wrapped character stream */
        private CodePointCharStream wrapped;

        /**
         * Constructs an uppercase character stream
         * 
         * @param wrapped the underlying character stream to wrap
         */
        public UpperCaseCharStream(CodePointCharStream wrapped) {
            this.wrapped = wrapped;
        }

        /**
         * Gets text from the specified interval
         * 
         * @param interval the text interval
         * @return the text content
         */
        @Override
        public String getText(Interval interval) {
            return wrapped.getText(interval);
        }

        /**
         * Consumes the next character from the stream
         */
        @Override
        public void consume() {
            wrapped.consume();
        }

        /**
         * Looks ahead at the specified character position
         * Converts the character to uppercase for case-insensitive parsing
         * 
         * @param i the lookahead position
         * @return the uppercase character at the position
         */
        @Override
        public int LA(int i) {
            int la = wrapped.LA(i);
            if (la == 0 || la == IntStream.EOF) {
                return la;
            }
            return Character.toUpperCase(la);
        }

        /**
         * Marks the current position in the stream
         * 
         * @return the mark position
         */
        @Override
        public int mark() {
            return wrapped.mark();
        }

        /**
         * Releases the mark at the specified position
         * 
         * @param marker the mark position to release
         */
        @Override
        public void release(int marker) {
            wrapped.release(marker);
        }

        /**
         * Gets the current index in the stream
         * 
         * @return the current index
         */
        @Override
        public int index() {
            return wrapped.index();
        }

        /**
         * Seeks to the specified index in the stream
         * 
         * @param index the target index
         */
        @Override
        public void seek(int index) {
            // Implementation needed for proper stream navigation
        }

        /**
         * Gets the size of the stream
         * 
         * @return the stream size
         */
        @Override
        public int size() {
            return wrapped.size();
        }

        /**
         * Gets the source name of the stream
         * 
         * @return the source name
         */
        @Override
        public String getSourceName() {
            return wrapped.getSourceName();
        }
    }
    
    /**
     * Custom error listener for parsing errors
     * Handles syntax errors during SQL parsing
     */
    public static class ParseErrorListener extends BaseErrorListener {
        
        /**
         * Handles syntax errors during parsing
         * 
         * @param recognizer the recognizer that detected the error
         * @param offendingSymbol the symbol that caused the error
         * @param line the line number where the error occurred
         * @param charPositionInLine the character position in the line
         * @param msg the error message
         * @param e the recognition exception
         */
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine,
                                String msg, RecognitionException e) {
            if (offendingSymbol instanceof CommonToken) {
                // TODO: Implement proper error handling for syntax errors
                // This could include logging, error reporting, or custom exception throwing
            }
        }
    }
}
