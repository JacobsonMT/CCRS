package com.jacobsonmt.ccrs.rest;

import com.jacobsonmt.ccrs.exceptions.FASTAValidationException;
import com.jacobsonmt.ccrs.model.CCRSJob;
import com.jacobsonmt.ccrs.model.FASTASequence;
import com.jacobsonmt.ccrs.services.JobManager;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Endpoints to access and submit jobs.
 *
 * TODO: Consider isolating clients so that they cannot view each others jobs.
 */
@Log4j2
@RequestMapping("/api/job")
@RestController
public class JobEndpoint {

    @Autowired
    private JobManager jobManager;

    @RequestMapping(value = "/{jobId}", method = RequestMethod.GET, produces = {MediaType.APPLICATION_JSON_VALUE})
    public CCRSJob.CCRSJobVO getJob( @PathVariable String jobId) {
        return createJobValueObject( jobManager.getSavedJob( jobId ) );
    }

    @RequestMapping(value = "/", method = RequestMethod.GET, produces = {MediaType.APPLICATION_JSON_VALUE})
    public CCRSJob.CCRSJobVO getJob2( @RequestParam(value = "jobId") String jobId) {
        return createJobValueObject( jobManager.getSavedJob( jobId ) );
    }

    @RequestMapping(value = "/{jobId}/status", method = RequestMethod.GET, produces = {MediaType.TEXT_PLAIN_VALUE})
    public String getJobStatus(@PathVariable String jobId) {
        CCRSJob job = jobManager.getSavedJob( jobId );

        if ( job != null ) {
            return job.getStatus();
        }

        log.info( "Job Not Found" );
        return "Job Not Found";
    }

    @RequestMapping(value = "/status", method = RequestMethod.GET, produces = {MediaType.TEXT_PLAIN_VALUE})
    public String getJobStatus2(@RequestParam(value = "jobId") String jobId) {
        return getJobStatus( jobId );
    }

    @RequestMapping(value = "/submit", method = RequestMethod.POST, produces = {MediaType.APPLICATION_JSON_VALUE})
    public ResponseEntity<Map<String,Object>> submitJob( @Valid @RequestBody JobSubmissionContent jobSubmissionContent, HttpServletRequest request) {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        String client = authentication.getName();

        LinkedHashMap<String, Object> response = new LinkedHashMap<>();

        Set<FASTASequence> sequences;
        HttpStatus status = HttpStatus.ACCEPTED;
        String msg = "";

        try {
            sequences = FASTASequence.parseFASTAContent( jobSubmissionContent.fastaContent );
            List<CCRSJob> jobs = jobManager.createJobs( client,
                    jobSubmissionContent.userId,
                    jobSubmissionContent.label,
                    sequences,
                    jobSubmissionContent.email,
                    jobSubmissionContent.hidden,
                    jobSubmissionContent.emailJobLinkPrefix,
                    jobSubmissionContent.emailOnJobSubmitted,
                    jobSubmissionContent.emailOnJobStart,
                    jobSubmissionContent.emailOnJobComplete
            );

            for ( CCRSJob job : jobs ) {
                jobManager.submit( job );
            }

            long errorCnt = sequences.stream().filter( s -> !s.getValidationStatus().isEmpty() ).count();
            if ( errorCnt > 0) {
                msg = errorCnt + " job(s) had a validation error";
            }
            response.put( "jobIds", jobs.stream().map( CCRSJob::getJobId ).collect( Collectors.toList() ) );

        } catch ( FASTAValidationException e ) {
            status = HttpStatus.BAD_REQUEST;
            msg = e.getMessage();
        }

        response.put( "message", msg );

        return ResponseEntity.status( status ).body(response);

    }

    @GetMapping("/{jobId}/delete")
    public ResponseEntity<String> stopJob( @PathVariable("jobId") String jobId) {
        CCRSJob job = jobManager.getSavedJob( jobId );

        // test for not null and complete
        if ( job != null ) {
            jobManager.stopJob( job );
            return ResponseEntity.ok("Job Deleted");
        }
        return ResponseEntity.badRequest().body( "" );
    }

    @GetMapping("/{jobId}/resultCSV")
    public ResponseEntity<String> jobResultCSV( @PathVariable("jobId") String jobId) {
        CCRSJob job = jobManager.getSavedJob( jobId );

        // test for not null and complete
        if ( job != null && job.isComplete() && !job.isFailed() ) {
            return createStreamingResponse(job.getResult().getResultCSV(), job.getLabel() + "-result.csv");
        }
        return ResponseEntity.badRequest().body( "" );
    }

    @GetMapping("/{jobId}/inputFASTA")
    public ResponseEntity<String> jobInputFASTA( @PathVariable("jobId") String jobId) {
        CCRSJob job = jobManager.getSavedJob( jobId );

        // test for not null and complete
        if ( job != null ) {
            return createStreamingResponse(job.getInputFASTAContent(), job.getLabel() + "-input.fasta");
        }
        return ResponseEntity.badRequest().body( "" );
    }

    private ResponseEntity<String> createStreamingResponse( String content, String filename ) {
        return ResponseEntity.ok()
                .contentType( MediaType.parseMediaType("application/octet-stream"))
                .header( HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"")
                .body(content);
    }

    private CCRSJob.CCRSJobVO createJobValueObject( CCRSJob job) {
        if ( job == null ) {
            return null;
        }
        return job.toValueObject(true, true);
    }

    @Getter
    @AllArgsConstructor
    private static final class JobSubmissionContent {
        private final String label;
        @NotBlank(message = "User missing!")
        private final String userId;
        @NotBlank(message = "FASTA content missing!")
        private final String fastaContent;
        private final Boolean hidden;
        private final String email;
        private final String emailJobLinkPrefix;
        private final Boolean emailOnJobSubmitted = false;
        private final Boolean emailOnJobStart = false;
        private final Boolean emailOnJobComplete = true;
    }


}
