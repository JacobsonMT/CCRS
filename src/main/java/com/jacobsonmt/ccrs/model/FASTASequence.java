package com.jacobsonmt.ccrs.model;

import com.jacobsonmt.ccrs.exceptions.FASTAValidationException;
import com.jacobsonmt.ccrs.exceptions.SequenceValidationException;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.logging.log4j.util.Strings;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

@Getter
@Setter
@RequiredArgsConstructor
@EqualsAndHashCode( of = "header" )
public class FASTASequence {

    private final String header;
    private final String sequence;
    private String validationStatus = "";

    public static final Set<Character> VALID_CHARACTERS = "QACDEFGHIKLMNPWRSTVYUOBJZ*X-.".chars()
            .mapToObj( e -> ( char ) e ).collect( Collectors.toSet() );

    private static final int MINIMUM_SEQUENCE_SIZE = 26;
    private static final int MAXIMUM_SEQUENCE_SIZE = 2000;

    public String getFASTAContent() {
        return header + '\r' + '\n' + sequence;
    }

    public static Set<FASTASequence> parseFASTAContent( String fasta ) throws FASTAValidationException {

        try {

            // Remove all empty lines
            fasta = fasta.replaceAll( "(?m)^[ \t]*\r?\n", "" );

            if ( Strings.isBlank( fasta ) ) {
                throw new FASTAValidationException( "Empty FASTA" );
            }

            Set<FASTASequence> sequences = new LinkedHashSet<>();

            String[] lines = fasta.split( "\\r?\\n" );

            if ( lines.length % 2 != 0 ) {
                throw new FASTAValidationException( "Unmatched or missing header(s) and/or sequence(s)" );
            }

            for ( int i = 0; i < lines.length; i += 2 ) {

                FASTASequence sequence = new FASTASequence( lines[i], lines[i + 1] );

                try {

                    if ( !sequence.getHeader().startsWith( ">" ) ) {
                        throw new SequenceValidationException( "Malformed header: " + sequence.getHeader() );
                    }

                    if ( sequences.contains( sequence ) ) {
                        throw new SequenceValidationException( "Duplicate header line: " + sequence.getHeader() );
                    }

                    if ( sequence.getSequence().length() < MINIMUM_SEQUENCE_SIZE ) {
                        throw new SequenceValidationException( "Sequence too short; minimum size is " + MINIMUM_SEQUENCE_SIZE );
                    }

                    if ( sequence.getSequence().length() > MAXIMUM_SEQUENCE_SIZE ) {
                        throw new SequenceValidationException( "Sequence too long; maximum size is " + MAXIMUM_SEQUENCE_SIZE );
                    }

//            if (!sequence.getSequence().matches( "^[\\QACDEFGHIKLMNPWRSTVYUOBJZ*X-.\\E]+$" )) {

                    int idx = 0;
                    for ( Character c : sequence.getSequence().toCharArray() ) {
                        if ( !FASTASequence.VALID_CHARACTERS.contains( c ) ) {
                            throw new SequenceValidationException( "Unknown character (" + c + ") at position (" + idx + ")" );
                        }
                        idx++;
                    }

                } catch ( SequenceValidationException sve ) {
                    sequence.setValidationStatus( sve.getMessage() );
                }

                sequences.add( sequence );

            }

            return sequences;
        } catch ( FASTAValidationException fve ) {
            throw fve;
        } catch ( Exception e ) {
            throw new FASTAValidationException( "Unknown validation error" );
        }

    }
}
