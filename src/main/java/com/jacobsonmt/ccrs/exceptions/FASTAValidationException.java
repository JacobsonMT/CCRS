package com.jacobsonmt.ccrs.exceptions;

public class FASTAValidationException extends RuntimeException {

    public FASTAValidationException( String message ) {
        super( message );
    }

    public FASTAValidationException( Throwable cause ) {
        super( cause );
    }

    public FASTAValidationException( String message, Throwable cause ) {
        super( message, cause );
    }
}
