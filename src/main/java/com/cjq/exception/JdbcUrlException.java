package com.cjq.exception;

public class JdbcUrlException extends RuntimeException{
    public JdbcUrlException() {
        super();
    }

    public JdbcUrlException(String message) {
        super(message);
    }

    public JdbcUrlException(String message, Throwable cause) {
        super(message, cause);
    }

    public JdbcUrlException(Throwable cause) {
        super(cause);
    }

    protected JdbcUrlException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
