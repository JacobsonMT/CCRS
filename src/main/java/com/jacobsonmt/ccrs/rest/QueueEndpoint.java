package com.jacobsonmt.ccrs.rest;

import com.jacobsonmt.ccrs.model.CCRSJob;
import com.jacobsonmt.ccrs.services.JobManager;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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

    @DeleteMapping("/client/{clientId}/user/{userId}/jobs/delete")
    public ResponseEntity<String> stopJobs( @PathVariable String clientId, @PathVariable String userId ) {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        String client = authentication.getName();
        if ( !client.equals( clientId ) && !client.equals( "admin" ) ) {
            return ResponseEntity.status( HttpStatus.UNAUTHORIZED ).body( "" );
        }

        String res = jobManager.stopJobs( clientId, userId );
        return ResponseEntity.accepted().body( res ); // Could be 'OK' as well, this seems semantically safer
    }

}
