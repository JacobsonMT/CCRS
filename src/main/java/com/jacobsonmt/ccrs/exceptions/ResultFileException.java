package com.jacobsonmt.ccrs.exceptions;

public class ResultFileException extends RuntimeException {

    public ResultFileException( String message ) {
        super( message );
    }

    public ResultFileException( Throwable cause ) {
        super( cause );
    }

    public ResultFileException( String message, Throwable cause ) {
        super( message, cause );
    }
}
