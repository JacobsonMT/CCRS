package com.jacobsonmt.ccrs.rest;

import com.jacobsonmt.ccrs.model.CCRSJob;
import com.jacobsonmt.ccrs.model.FASTASequence;
import com.jacobsonmt.ccrs.services.JobManager;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@Log4j2
@RequestMapping("/api/queue")
@RestController
public class QueueEndpoint {

    @Autowired
    private JobManager jobManager;

    /**
     * @return Approximate number of jobs that have completed for a client. Can be used to test when to update during polling.
     */
    @RequestMapping(value = "/client/{clientId}/complete", method = RequestMethod.GET, produces = {MediaType.APPLICATION_JSON_VALUE})
    public ResponseEntity<Integer> getCompletionCount( @PathVariable String clientId ) {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        String client = authentication.getName();
        if ( !client.equals( clientId ) && !client.equals( "admin" ) ) {
            return ResponseEntity.status( HttpStatus.UNAUTHORIZED ).body( null );
        }
        return ResponseEntity.ok( jobManager.getCompletionCount( clientId ) );
    }

//    @RequestMapping(value = "/public", method = RequestMethod.GET, produces = {MediaType.APPLICATION_JSON_VALUE})
//    public ResponseEntity<List<CCRSJob.CCRSJobVO>> getJobs( @RequestParam(value = "withResults", defaultValue = "false") boolean withResults ) {
//        return ResponseEntity.ok( jobManager.listPublicJobs( withResults ) );
//    }

//    @RequestMapping(value = "/client/{clientId}", method = RequestMethod.GET, produces = {MediaType.APPLICATION_JSON_VALUE})
//    public ResponseEntity<List<CCRSJob.CCRSJobVO>> getJobs( @PathVariable String clientId,
//                                            @RequestParam(value = "withResults", defaultValue = "false") boolean withResults ) {
//        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
//        String client = authentication.getName();
//        if ( !client.equals( clientId ) && !client.equals( "admin" ) ) {
//            return ResponseEntity.status( HttpStatus.UNAUTHORIZED ).body( new ArrayList<>() );
//        }
//        return ResponseEntity.ok( jobManager.listJobsForClient( clientId, withResults) );
//    }

    @RequestMapping(value = "/client/{clientId}/user/{userId}", method = RequestMethod.GET, produces = {MediaType.APPLICATION_JSON_VALUE})
    public ResponseEntity<List<CCRSJob.CCRSJobVO>> getJobs( @PathVariable String clientId, @PathVariable String userId,
                                            @RequestParam(value = "withResults", defaultValue = "false") boolean withResults  ) {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        String client = authentication.getName();
        if ( !client.equals( clientId ) && !client.equals( "admin" ) ) {
            return ResponseEntity.status( HttpStatus.UNAUTHORIZED ).body( new ArrayList<>() );
        }
        return ResponseEntity.ok( jobManager.listJobsForClientAndUser( clientId, userId, withResults) );
    }

    @RequestMapping(value = "/mock", method = RequestMethod.GET, produces = {MediaType.APPLICATION_JSON_VALUE})
    public String createMockJobs() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        String client = authentication.getName();
        if ( client.equals( "admin" ) ) {
            for ( int i = 0; i < 5; i++ ) {
                for ( int j = 0; j < 5; j++ ) {
                    CCRSJob job = jobManager.createJob( "client" + (1 + i % 2),
                            "user" + j,
                            i + "label" + j,
                            new FASTASequence( "Mock Header", i + " fasta content " + j, "" ) ,
                            i + "email" + j,
                            i % 3 == 0,
                            "",
                            false,
                            false,
                            false );
                    jobManager.submit( job );
                }
            }
            return "Success";
        }
        return "Failure";
    }

}
