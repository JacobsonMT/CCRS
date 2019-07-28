package com.jacobsonmt.ccrs.rest;

import com.jacobsonmt.ccrs.exceptions.FASTAValidationException;
import com.jacobsonmt.ccrs.model.CCRSJob;
import com.jacobsonmt.ccrs.model.FASTASequence;
import com.jacobsonmt.ccrs.model.Message;
import com.jacobsonmt.ccrs.services.JobManager;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.validation.BindingResult;
import org.springframework.validation.ObjectError;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import javax.validation.constraints.Email;
import javax.validation.constraints.NotBlank;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
    public ResponseEntity<CCRSJob.CCRSJobVO> getJob( @PathVariable String jobId) {
        CCRSJob job = jobManager.getSavedJob( jobId );

        if ( job == null ) {
            return ResponseEntity.notFound().build();
        }

        return ResponseEntity.ok( createJobValueObject( jobManager.getSavedJob( jobId ) ) );
    }

    @RequestMapping(value = "/{jobId}/status", method = RequestMethod.GET, produces = {MediaType.TEXT_PLAIN_VALUE})
    public ResponseEntity<String> getJobStatus(@PathVariable String jobId) {
        CCRSJob job = jobManager.getSavedJob( jobId );

        if ( job == null ) {
            return ResponseEntity.status( HttpStatus.NOT_FOUND ).body( "Job Not Found" );
        }

        return ResponseEntity.ok( job.getStatus() );
    }

    @RequestMapping(value = "/submit", method = RequestMethod.POST, produces = {MediaType.APPLICATION_JSON_VALUE})
    public ResponseEntity<JobSubmissionResponse> submitJob( @Valid @RequestBody JobSubmissionContent jobSubmissionContent, BindingResult errors ) {
        // NOTE: You must declare an Errors, or BindingResult argument immediately after the validated method argument.
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        String client = authentication.getName();

        JobSubmissionResponse result = new JobSubmissionResponse();

        if (errors.hasErrors()) {
            for ( ObjectError error : errors.getAllErrors() ) {
                result.addMessage( new Message( Message.MessageLevel.ERROR, error.getDefaultMessage() ) );
            }
        } else {

            try {
                Set<FASTASequence> sequences = FASTASequence.parseFASTAContent( jobSubmissionContent.fastaContent );
                result.setTotalSubmittedJobs( sequences.size() );

                sequences.stream().filter( s -> !s.getValidationStatus().isEmpty() ).forEach(
                        s -> {
                            result.addRejectedHeader( s.getHeader() );
                            result.addMessage( new Message( Message.MessageLevel.WARNING, s.getValidationStatus() + " for '" + s.getHeader() + "'" ) );
                        }
                );

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
                    String rejectedMsg = jobManager.submit( job );
                    if ( rejectedMsg.isEmpty() ) {
                        result.addAcceptedJob( job );
                    }
                }

                if ( result.getAcceptedJobs().isEmpty() ) {
                    result.addMessage( new Message( Message.MessageLevel.WARNING, "No jobs were submitted." ) );
                } else {
                    result.addMessage( new Message( Message.MessageLevel.INFO, "Submitted " + result.getAcceptedJobs().size() + " jobs." ) );
                }

            } catch ( FASTAValidationException e ) {
                result.addMessage( new Message( Message.MessageLevel.ERROR, e.getMessage() ) );
            }
        }

        HttpStatus status = result.getMessages().stream()
                .anyMatch( m -> m.getLevel().equals( Message.MessageLevel.ERROR ) ) ?
                HttpStatus.BAD_REQUEST :
                HttpStatus.OK;

        return ResponseEntity.status( status ).body( result );

    }

    @DeleteMapping("/{jobId}/delete")
    public ResponseEntity<String> stopJob( @PathVariable("jobId") String jobId) {
        CCRSJob job = jobManager.getSavedJob( jobId );

        if ( job == null ) {
            return ResponseEntity.status( HttpStatus.NOT_FOUND ).body( "Job Not Found" );
        }

        jobManager.stopJob( job );
        return ResponseEntity.accepted().body( "Job Delete: " + jobId ); // Could be 'OK' as well, this seems semantically safer
    }

    @GetMapping("/{jobId}/resultCSV")
    public ResponseEntity<String> jobResultCSV( @PathVariable("jobId") String jobId) {
        CCRSJob job = jobManager.getSavedJob( jobId );

        if ( job == null ) {
            return ResponseEntity.status( HttpStatus.NOT_FOUND ).body( "Job Not Found" );
        }

        if ( !job.isComplete() ) {
            return ResponseEntity.status( HttpStatus.PROCESSING ).body( "Not Yet Complete");
        }

        if ( job.isFailed() ) {
            return ResponseEntity.status( HttpStatus.NOT_FOUND ).body( "Job Failed" );
        }

        return createStreamingResponse(job.getResult().getResultCSV(), job.getLabel() + ".list");
    }

    @GetMapping("/{jobId}/inputFASTA")
    public ResponseEntity<String> jobInputFASTA( @PathVariable("jobId") String jobId) {
        CCRSJob job = jobManager.getSavedJob( jobId );

        if ( job == null ) {
            return ResponseEntity.status( HttpStatus.NOT_FOUND ).body( "Job Not Found" );
        }

        return createStreamingResponse(job.getInputFASTAContent(), job.getLabel() + ".fasta");
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
        @Email(message = "Not a valid email address")
        private final String email;
        private final String emailJobLinkPrefix;
        private final Boolean emailOnJobSubmitted = false;
        private final Boolean emailOnJobStart = false;
        private final Boolean emailOnJobComplete = true;
    }

    @Setter
    @Getter
    @NoArgsConstructor
    static class JobSubmissionResponse {
        private List<Message> messages = new ArrayList<>();
        private List<CCRSJob.CCRSJobVO> acceptedJobs = new ArrayList<>();;
        private List<String> rejectedJobHeaders = new ArrayList<>();;
        private int totalSubmittedJobs;

        private void addMessage( Message message) {
            messages.add( message );
        }

        private void addAcceptedJob(CCRSJob job) {
            acceptedJobs.add( job.toValueObject( false, false ) );
        }

        private void addRejectedHeader(String label) {
            rejectedJobHeaders.add( label );
        }
    }


}
