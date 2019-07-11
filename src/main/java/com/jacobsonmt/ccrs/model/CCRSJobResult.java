package com.jacobsonmt.ccrs.model;

import lombok.Getter;
import lombok.Setter;

import java.io.BufferedReader;
import java.io.StringReader;

@Getter
@Setter
public final class CCRSJobResult {

    private final String resultCSV;
    private final int taxaId;

    /**
     * Used to create a slim result with no content
     *
     * @param taxaId OS taxa id
     */
    CCRSJobResult( int taxaId ) {
        this.taxaId = taxaId;
        resultCSV = "";
    }

    public CCRSJobResult( String resultCSV ) {
        this.resultCSV = resultCSV;

        // Parse first line
        int tid = -1;
        try ( BufferedReader reader = new BufferedReader(new StringReader(resultCSV))) {
            String line = reader.readLine();

            if ( line != null && line.startsWith( "OX\t" )) {
                String[] sline = line.split( "\t" );
                tid = Integer.valueOf( sline[1] );
            }

        } catch ( Exception e) {
            // pass
        }

        taxaId = tid;

    }
}
