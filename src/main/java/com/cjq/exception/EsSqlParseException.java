package com.cjq.exception;

public class EsSqlParseException extends RuntimeException{
    public EsSqlParseException() {
        super();
    }

    public EsSqlParseException(String message) {
        super(message);
    }

    public EsSqlParseException(String message, Throwable cause) {
        super(message, cause);
    }

    public EsSqlParseException(Throwable cause) {
        super(cause);
    }

    protected EsSqlParseException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
