package com.jacobsonmt.ccrs.model;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith( SpringRunner.class )
@SpringBootTest
public class CCRSJobResultTest {

    private static String baseCSVResult = "AC\tPos\tRef\tDepth\tConservation\tA\tR\tN\tD\tC\tQ\tE\tG\tH\tI\tL\tK\tM\tF\tP\tS\tT\tW\tY\tV\n" +
            "sp|P07766|CD3E_\t1\tM\t41\t0.785\t0.785\t0.785\t0.785\t0.785\t0.785\t0.785\t0.785\t0.785\t0.785\t0.785\t0.785\t0.785\t0\t0.785\t0.785\t0.785\t0.785\t0.785\t0.785\t0.785\n" +
            "sp|P07766|CD3E_\t2\tQ\t7\t0.253307\t0.244276\t0.233177\t0.244276\t0.244276\t0.244276\t0\t0.317168\t0.244276\t0.281372\t0.244276\t0.232177\t0.30097\t0.244276\t0.244276\t0.272373\t0.244276\t0.244276\t0.244276\t0.244276\t0.244276";

    @Test
    public void parseTaxaIdWhenExists() {
        String resultCSV = "OX\t9749\n" + baseCSVResult;
        CCRSJobResult result = new CCRSJobResult( resultCSV );
        assertThat( result.getTaxaId() ).isEqualTo( 9749 );
    }

    @Test
    public void parseTaxaIdWhenNotExists() {
        String resultCSV = baseCSVResult;
        CCRSJobResult result = new CCRSJobResult( resultCSV );
        assertThat( result.getTaxaId() ).isEqualTo( -1 );
    }

    @Test
    public void parseTaxaIdWhenExistsAndMalformed1() {
        String resultCSV = "wsersdfOX\t9749\n" + baseCSVResult;
        CCRSJobResult result = new CCRSJobResult( resultCSV );
        assertThat( result.getTaxaId() ).isEqualTo( -1 );
    }

    @Test
    public void parseTaxaIdWhenExistsAndMalformed2() {
        String resultCSV = "OX 9749\n" + baseCSVResult;
        CCRSJobResult result = new CCRSJobResult( resultCSV );
        assertThat( result.getTaxaId() ).isEqualTo( -1 );
    }

    @Test
    public void parseTaxaIdWhenExistsAndMalformed3() {
        String resultCSV = "OXasdasf\t9749\n" + baseCSVResult;
        CCRSJobResult result = new CCRSJobResult( resultCSV );
        assertThat( result.getTaxaId() ).isEqualTo( -1 );
    }

    @Test
    public void parseSlim() {
        CCRSJobResult result = new CCRSJobResult( 5 );
        assertThat( result.getTaxaId() ).isEqualTo( 5 );
        assertThat( result.getResultCSV() ).isEqualTo( "" );
    }


}
