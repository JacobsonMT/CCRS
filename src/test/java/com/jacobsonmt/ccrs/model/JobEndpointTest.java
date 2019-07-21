package com.jacobsonmt.ccrs.model;

import com.jacobsonmt.ccrs.services.JobManager;
import com.jacobsonmt.ccrs.settings.ClientSettings;
import org.assertj.core.util.Maps;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import static org.hamcrest.CoreMatchers.is;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith( SpringRunner.class )
@WebMvcTest
@AutoConfigureMockMvc
@TestPropertySource(
        locations = {"classpath:application.properties"},
        properties = {"spring.jackson.date-format=yyyy-MM-dd HH:mm:ss z", "spring.jackson.time-zone=UTC"})
@ContextConfiguration(
        initializers={ConfigFileApplicationContextInitializer.class}
)
public class JobEndpointTest {

    @Autowired
    private MockMvc mvc;

    @MockBean
    private ClientSettings clientSettings;

    @MockBean
    private JobManager jobManager;

    @Autowired private ApplicationContext ctx;

    private CCRSJob commonJob;

    private SimpleDateFormat jacksonDateFormat;

    @Before
    public void setUp() {

        CCRSJob.CCRSJobBuilder builder = CCRSJob.builder();

        builder.jobId( "testJobId" );
        builder.clientId( "testClient" );
        builder.label( "testLabel" );
        builder.status( "testStatus" );
        builder.running( false );
        builder.failed( false );
        builder.complete( true );
        builder.position( null );
        builder.email( "email@email.com" );
        builder.hidden( true );
        builder.submittedDate( new Date() );
        builder.startedDate( new Date() );
        builder.finishedDate( new Date() );
        builder.inputFASTAContent( ">Example Header\nMQSGTHWRVLGLCLLSVGVWGQDGNEEMGGITQTPYKVSISGTTVILTCPQYPGSEILW\n" );
        builder.result( new CCRSJobResult( "OX\t9749\n" + "AC\tPos\tRef\tDepth\tConservation\tA\tR\tN\tD\tC\tQ\tE\tG\tH\tI\tL\tK\tM\tF\tP\tS\tT\tW\tY\tV\n" +
                "sp|P07766|CD3E_\t1\tM\t41\t0.785\t0.785\t0.785\t0.785\t0.785\t0.785\t0.785\t0.785\t0.785\t0.785\t0.785\t0.785\t0.785\t0\t0.785\t0.785\t0.785\t0.785\t0.785\t0.785\t0.785\n" +
                "sp|P07766|CD3E_\t2\tQ\t7\t0.253307\t0.244276\t0.233177\t0.244276\t0.244276\t0.244276\t0\t0.317168\t0.244276\t0.281372\t0.244276\t0.232177\t0.30097\t0.244276\t0.244276\t0.272373\t0.244276\t0.244276\t0.244276\t0.244276\t0.244276" ));
        builder.executionTime( 17 );

        commonJob = builder.build();

        ClientSettings.ApplicationClient client = new ClientSettings.ApplicationClient();
        client.setToken( "testclienttoken" );
        given( clientSettings.getClients() ).willReturn( Maps.newHashMap("testclient", client ));

        given( jobManager.getSavedJob( commonJob.getJobId() ) ).willReturn( commonJob );

        jacksonDateFormat = new SimpleDateFormat( ctx.getEnvironment().getProperty( "spring.jackson.date-format" ) );
        jacksonDateFormat.setTimeZone( TimeZone.getTimeZone( ctx.getEnvironment().getProperty( "spring.jackson.time-zone" ) ));

    }

    @Test
    public void givenJobExists_whenGetJob_thenReturnJson() throws Exception {

        mvc.perform( get( "/api/job/" + commonJob.getJobId() )
                .contentType( MediaType.APPLICATION_JSON )
                .header( "auth_token", "testclienttoken" )
                .header( "client", "testclient" ))
                .andExpect( status().isOk() )
                .andExpect( jsonPath( "$.jobId", is( commonJob.getJobId())))
                .andExpect( jsonPath( "$.clientId", is( commonJob.getClientId())))
                .andExpect( jsonPath( "$.label", is( commonJob.getLabel())))
                .andExpect( jsonPath( "$.status", is( commonJob.getStatus())))
                .andExpect( jsonPath( "$.running", is( commonJob.isRunning())))
                .andExpect( jsonPath( "$.failed", is( commonJob.isFailed())))
                .andExpect( jsonPath( "$.complete", is( commonJob.isComplete())))
                .andExpect( jsonPath( "$.position", is( commonJob.getPosition())))
                .andExpect( jsonPath( "$.email", is( CCRSJob.obfuscateEmail(commonJob.getEmail()))))
                .andExpect( jsonPath( "$.hidden", is( commonJob.isHidden())))
                .andExpect( jsonPath( "$.submittedDate", is( jacksonDateFormat.format(commonJob.getSubmittedDate()))))
                .andExpect( jsonPath( "$.startedDate", is( jacksonDateFormat.format(commonJob.getStartedDate()))))
                .andExpect( jsonPath( "$.finishedDate", is( jacksonDateFormat.format(commonJob.getFinishedDate()))))
                .andExpect( jsonPath( "$.inputFASTAContent", is( commonJob.getInputFASTAContent())))
                .andExpect( jsonPath( "$.result.resultCSV", is( commonJob.getResult().getResultCSV())))
                .andExpect( jsonPath( "$.result.taxaId", is( commonJob.getResult().getTaxaId())));

    }

    @Test
    public void givenJobNotExists_whenGetJob_thenReturn404() throws Exception {
        mvc.perform( get( "/api/job/" + commonJob.getJobId() + "wrong" )
                .contentType( MediaType.APPLICATION_JSON )
                .header( "auth_token", "testclienttoken" )
                .header( "client", "testclient" ))
                .andExpect( status().isNotFound() );
    }

    @Test
    public void givenWrongCredentials_whenGetJob_thenReturn403() throws Exception {
        mvc.perform( get( "/api/job/" + commonJob.getJobId() )
                .contentType( MediaType.APPLICATION_JSON )
                .header( "auth_token", "testclienttokenwrong" )
                .header( "client", "testclientwrong" ))
                .andExpect( status().isForbidden() );
    }

    /* Status */

    @Test
    public void givenJobExists_whenGetJobStatus_thenReturnString() throws Exception {

        mvc.perform( get( "/api/job/" + commonJob.getJobId() + "/status" )
                .contentType( MediaType.APPLICATION_JSON )
                .header( "auth_token", "testclienttoken" )
                .header( "client", "testclient" ))
                .andExpect( status().isOk() )
                .andExpect( jsonPath( "$", is( commonJob.getStatus())));
    }

    @Test
    public void givenJobNotExists_whenGetJobStatus_thenReturn404() throws Exception {
        mvc.perform( get( "/api/job/" + commonJob.getJobId() + "wrong" + "/status" )
                .contentType( MediaType.APPLICATION_JSON )
                .header( "auth_token", "testclienttoken" )
                .header( "client", "testclient" ))
                .andExpect( status().isNotFound() );
    }

    @Test
    public void givenWrongCredentials_whenGetJobStatus_thenReturn403() throws Exception {
        mvc.perform( get( "/api/job/" + commonJob.getJobId() + "/status" )
                .contentType( MediaType.APPLICATION_JSON )
                .header( "auth_token", "testclienttokenwrong" )
                .header( "client", "testclientwrong" ))
                .andExpect( status().isForbidden() );
    }

//    /* Submit */
//
//    @Test
//    public void whenSubmitSingleJob_thenReturnEmptyMessage() throws Exception {
//        mvc.perform( post( "/api/job/submit" )
//                .contentType( MediaType.APPLICATION_JSON )
//                .header( "auth_token", "testclienttoken" )
//                .header( "client", "testclient" )
//                .content(  ))
//                .andExpect( status().isOk() )
//                .andExpect( jsonPath( "$", is( commonJob.getStatus())));
//    }

}
