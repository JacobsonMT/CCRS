package com.jacobsonmt.ccrs.rest;

import com.jacobsonmt.ccrs.model.CCRSJob;
import com.jacobsonmt.ccrs.services.JobManager;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Log4j2
@RequestMapping("/api/queue")
@RestController
public class QueueEndpoint {

    @Autowired
    private JobManager jobManager;

    @RequestMapping(value = "/public", method = RequestMethod.GET, produces = {MediaType.APPLICATION_JSON_VALUE})
    public List<CCRSJob.CCRSJobVO> getJob() {
        return jobManager.listPublicJobs();
    }

    @RequestMapping(value = "/mock", method = RequestMethod.GET, produces = {MediaType.APPLICATION_JSON_VALUE})
    public void createMockJobs() {
        for ( int i = 0; i < 10; i++ ) {
            log.info( i );
            CCRSJob job = jobManager.createJob( "client" + i,
                    "label" + i,
                    "fasta content " + i,
                    "email" + 1,
                    i % 3 == 0 );
            jobManager.submit( job );
        }
    }

}
