package com.jacobsonmt.ccrs.exceptions;

public class SequenceValidationException extends RuntimeException {

    public SequenceValidationException( String message ) {
        super( message );
    }

    public SequenceValidationException( Throwable cause ) {
        super( cause );
    }

    public SequenceValidationException( String message, Throwable cause ) {
        super( message, cause );
    }
}
