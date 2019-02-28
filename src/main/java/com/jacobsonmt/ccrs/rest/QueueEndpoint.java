package com.jacobsonmt.ccrs.rest;

import com.jacobsonmt.ccrs.model.CCRSJob;
import com.jacobsonmt.ccrs.services.JobManager;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@Log4j2
@RequestMapping("/api/queue")
@RestController
public class QueueEndpoint {

    @Autowired
    private JobManager jobManager;

    @RequestMapping(value = "/public", method = RequestMethod.GET, produces = {MediaType.APPLICATION_JSON_VALUE})
    public List<CCRSJob.CCRSJobVO> getJobs() {
        return jobManager.listPublicJobs();
    }

    @RequestMapping(value = "/client/{clientId}", method = RequestMethod.GET, produces = {MediaType.APPLICATION_JSON_VALUE})
    public List<CCRSJob.CCRSJobVO> getJobs( @PathVariable String clientId ) {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        String client = authentication.getName();
        if ( client.equals( clientId ) || client.equals( "admin" ) ) {
            return jobManager.listJobsForClient(clientId);
        }
        return new ArrayList<>();
    }

    @RequestMapping(value = "/client/{clientId}/user/{userId}", method = RequestMethod.GET, produces = {MediaType.APPLICATION_JSON_VALUE})
    public List<CCRSJob.CCRSJobVO> getJobs( @PathVariable String clientId, @PathVariable String userId  ) {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        String client = authentication.getName();
        if ( client.equals( clientId ) || client.equals( "admin" ) ) {
            return jobManager.listJobsForClientAndUser(clientId, userId);
        }
        return new ArrayList<>();
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
                            i + " fasta content " + j,
                            i + "email" + j,
                            i % 3 == 0 );
                    jobManager.submit( job );
                }
            }
            return "Success";
        }
        return "Failure";
    }

}
