package com.jacobsonmt.ccrs.services;

import com.jacobsonmt.ccrs.model.CCRSJob;
import com.jacobsonmt.ccrs.model.CCRSJobResult;
import com.jacobsonmt.ccrs.model.FASTASequence;
import com.jacobsonmt.ccrs.model.PurgeOldJobs;
import com.jacobsonmt.ccrs.settings.ApplicationSettings;
import com.jacobsonmt.ccrs.settings.ClientSettings;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mail.MailSendException;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.mail.MessagingException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.jacobsonmt.ccrs.model.CCRSJob.inputStreamToString;

@Log4j2
@Service
public class JobManager {

    @Autowired
    ApplicationSettings applicationSettings;

    @Autowired
    ClientSettings clientSettings;

    @Autowired
    EmailService emailService;

    // Main executor to process jobs
    private ExecutorService executor;

    // Contains a copy of the processing queue of jobs internal to executor.
    // It is non-trivial to extract a list of running/waiting jobs in the executor
    // so we maintain a copy in sync with the real thing.
    private final Set<CCRSJob> jobQueueMirror = new LinkedHashSet<>();

    // Secondary client queues or waiting lines. One specific to each client.
    private Map<String, Queue<CCRSJob>> clientQueues = new ConcurrentHashMap<>();

    // Tertiary user queues or waiting lines. One specific to each user in each client.
    private Map<String, Queue<CCRSJob>> userQueues = new ConcurrentHashMap<>();

    private static String userQueueKey(CCRSJob job) {
        return job.getClientId() + "-" + job.getUserId();
    }

    // Contains map of token to saved job for future viewing
    private final Map<String, CCRSJob> savedJobs = new ConcurrentHashMap<>();

    // Stored approximate number of completed jobs for each clientId. Can be used to test when to update during polling.
    private final Map<String, AtomicInteger> completionCounts = new ConcurrentHashMap<>();

    // Used to periodically purge the old saved jobs
    private ScheduledExecutorService scheduler;

    private static final int MAX_DEFAULT_LABEL_SIZE = 20;

    /**
     * Initialize Job Manager:
     *
     * Create and schedule process queue executor.
     * Load previously completed jobs from disk if enabled.
     */
    @PostConstruct
    private void initialize() {
        log.info( "Job Manager Initialize" );
        executor = Executors.newFixedThreadPool( applicationSettings.getConcurrentJobs() );
        if ( applicationSettings.isPurgeSavedJobs() ) {
            scheduler = Executors.newSingleThreadScheduledExecutor();
            // Checks every hour for old jobs
            scheduler.scheduleAtFixedRate( new PurgeOldJobs( savedJobs ), 0,
                    applicationSettings.getPurgeSavedJobsTimeHours(), TimeUnit.HOURS );
        }

        if ( applicationSettings.isLoadJobsFromDisk() ) {

            // Populate completed jobs from jobs folder
            Path jobsDirectory = Paths.get( applicationSettings.getJobsDirectory() );

            PathMatcher matcher =
                    FileSystems.getDefault().getPathMatcher( "glob:**/" + applicationSettings.getJobSerializationFilename() );

            try {
                Files.walkFileTree( jobsDirectory, new SimpleFileVisitor<Path>() {

                    @Override
                    public FileVisitResult visitFile( Path path,
                                                      BasicFileAttributes attrs ) throws IOException {
                        if ( matcher.matches( path ) ) {
                            try ( ObjectInputStream ois = new ObjectInputStream( Files.newInputStream( path ) ) ) {
                                CCRSJob job = (CCRSJob) ois.readObject();

                                // Add back important transient fields
                                job.setJobsDirectory( path.getParent() );

                                job.setInputFASTAContent( inputStreamToString( Files.newInputStream( job.getJobsDirectory().resolve( job.getInputFASTAFilename() ) ) ) );

                                job.setPosition( null );
                                job.setEmail( "" );

                                CCRSJobResult result = new CCRSJobResult(
                                        inputStreamToString( Files.newInputStream( job.getJobsDirectory().resolve( job.getOutputCSVFilename() ) ) )
                                );
                                job.setResult( result );

                                job.setSaveExpiredDate( System.currentTimeMillis() + applicationSettings.getPurgeAfterHours() * 60 * 60 * 1000 );

                                saveJob( job );
                            } catch ( ClassNotFoundException e ) {
                                log.error( e );
                            }
                        }
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFileFailed( Path file, IOException exc )
                            throws IOException {
                        return FileVisitResult.CONTINUE;
                    }
                } );
            } catch ( IOException e ) {
                log.error( e );
            }

        }

    }

    @PreDestroy
    public void destroy() {
        log.info( "JobManager destroyed" );
        executor.shutdownNow();
        scheduler.shutdownNow();
    }


    /**
     * Create job with specified parameters but do not submit it to any queue.
     *
     * @param clientId Client of the job.
     * @param userId User from client.
     * @param label Short label/name for the job.
     * @param sequence Input sequence data used to run the job.
     * @param email Email to be used for notifications if enabled.
     * @param hidden Is job private.
     * @param emailJobLinkPrefix Link prefix used to create the job url in client
     * @param emailOnJobSubmitted Send email when job is submitted to the queueing system.
     * @param emailOnJobStart Send email when job is started.
     * @param emailOnJobComplete Send email when job is complete.
     * @return Created job.
     */
    public CCRSJob createJob( String clientId,
                              String userId,
                              String label,
                              FASTASequence sequence,
                              String email,
                              boolean hidden,
                              String emailJobLinkPrefix,
                              boolean emailOnJobSubmitted,
                              boolean emailOnJobStart,
                              boolean emailOnJobComplete ) {
        CCRSJob.CCRSJobBuilder jobBuilder = CCRSJob.builder();

        // Generated
        String jobId = UUID.randomUUID().toString();
        jobBuilder.jobId( jobId );

        // Static Resources
        jobBuilder.command( applicationSettings.getCommand() );
        jobBuilder.jobsDirectory( Paths.get( applicationSettings.getJobsDirectory(), jobId) );
        jobBuilder.outputCSVFilename( applicationSettings.getOutputCSVFilename() );
        jobBuilder.inputFASTAFilename( applicationSettings.getInputFASTAFilename() );
        jobBuilder.jobSerializationFilename( applicationSettings.getJobSerializationFilename() );

        // User Inputs
        jobBuilder.clientId( clientId );
        jobBuilder.userId( userId );
        jobBuilder.label( Strings.isNotBlank( label ) ? label :
                sequence.getHeader().substring( 0, Math.min( sequence.getHeader().length(), MAX_DEFAULT_LABEL_SIZE ) ) );
        jobBuilder.inputFASTAContent( sequence.getFASTAContent() );
        jobBuilder.hidden( hidden );
        jobBuilder.email( email );
        jobBuilder.emailJobLinkPrefix( emailJobLinkPrefix );
        jobBuilder.emailOnJobSubmitted( emailOnJobSubmitted );
        jobBuilder.emailOnJobStart( emailOnJobStart );
        jobBuilder.emailOnJobComplete( emailOnJobComplete );

        return jobBuilder.build();

    }

    public List<CCRSJob> createJobs( String clientId,
                                     String userId,
                                     String label,
                                     Set<FASTASequence> sequences,
                                     String email,
                                     boolean hidden,
                                     String emailJobLinkPrefix,
                                     boolean emailOnJobSubmitted,
                                     boolean emailOnJobStart,
                                     boolean emailOnJobComplete ) {

        List<CCRSJob> jobs = new ArrayList<>();


        for ( FASTASequence sequence : sequences ) {
            CCRSJob job = createJob(
                    clientId,
                    userId,
                    label,
                    sequence,
                    email,
                    hidden,
                    emailJobLinkPrefix,
                    emailOnJobSubmitted,
                    emailOnJobStart,
                    emailOnJobComplete );

            if ( !sequence.getValidationStatus().isEmpty() ) {
                job.setComplete( true );
                job.setFailed( true );
                job.setPosition( null ); // Mainly for sort order in tables
                job.setStatus( sequence.getValidationStatus() );
                saveJob( job ); // So the job's validation status can be queried
                log.info( "Validation error: " + job.getJobId() + " - " + job.getStatus() );
            }

            jobs.add( job );

        }

        return jobs;
    }

    /**
     * Submit job to executor for process queue.
     *
     * Synchronized on jobQueueMirror.
     *
     * @param job
     */
    private void submitToProcessQueue( CCRSJob job ) {
        synchronized ( jobQueueMirror ) {
            log.info( "Submitting job (" + job.getJobId() + ") for client-user: (" + userQueueKey(job) + ") to process queue" );
            job.setSubmittedDate( new Date() );
            job.setJobManager( this );

            jobQueueMirror.add( job );
            job.setPosition( (int) jobQueueMirror.stream().filter( j -> !j.isRunning() ).count() );
            job.setStatus( "Position: " + job.getPosition() );

            Future<CCRSJobResult> future = executor.submit( job );
            job.setFuture( future );

        }
    }

    /**
     * For a specific client, submit the job at the top of their personal queue to the process queue if there is room left in their
     * allocated process queue limit.
     *
     * @param clientId specific client
     */
    private void submitTopOfClientQueue( String clientId ) {
        int cnt = 0;
        synchronized ( jobQueueMirror ) {

            for ( CCRSJob job : jobQueueMirror ) {
                if ( job.getClientId().equals( clientId ) ) cnt++;
            }
        }

        if ( cnt < clientSettings.getClients().get( clientId ).getProcessLimit() ) {

            Queue<CCRSJob> jobs = clientQueues.get( clientId );

            if ( jobs != null ) {
                CCRSJob job;
                synchronized ( jobs ) {
                    job = jobs.poll();
                }
                if ( job != null ) {
                    submitToProcessQueue( job );
                }
            }
        }

    }

    /**
     * Add job to personal queue of owning client if there is room in their total job allocation limit.
     *
     * @param job
     */
    private void submitToClientQueue( CCRSJob job ) {
        log.info( "Submitting job (" + job.getJobId() + ") for client-user: (" + userQueueKey(job) + ") to client queue" );

        Queue<CCRSJob> jobs = clientQueues.computeIfAbsent( job.getClientId(), k -> new LinkedList<>() );

        if ( jobs.size() > clientSettings.getClients().get( job.getClientId() ).getJobLimit() ) {
            log.info( "Too many jobs (" + job.getJobId() + ") for client-user: (" + userQueueKey(job) + ")");
            return;
        }

        synchronized ( jobs ) {

            if ( !jobs.contains( job ) ) {
                jobs.add( job );
                job.setStatus( "Pending..." );
                submitTopOfClientQueue( job.getClientId() );
            }
        }
    }

    /**
     * For a specific user, submit the job at the top of their personal queue to their owning client queue
     * if there is room left in their allocated client queue limit.
     *
     * @param userQueueKey key to user queue map for user
     */
    private void submitTopOfUserQueue( String userQueueKey ) {

        Queue<CCRSJob> jobs = userQueues.get( userQueueKey );

        if ( jobs != null ) {
            CCRSJob job = jobs.peek();

            if ( job != null ) {

                Queue<CCRSJob> clientQueue = clientQueues.computeIfAbsent( job.getClientId(), k -> new LinkedList<>() );

                int cnt = 0;
                synchronized ( clientQueue ) {

                    for ( CCRSJob j : clientQueue ) {
                        if ( j.getUserId().equals( job.getUserId() ) ) cnt++;
                    }
                }

                if ( cnt < clientSettings.getClients().get( job.getClientId() ).getUserClientLimit() ) {
                    synchronized ( jobs ) {
                        job = jobs.poll();
                    }
                    submitToClientQueue( job );
                }
            }
        }
    }

    /**
     * Add job to personal queue of owning user if there is room in their total job allocation limit.
     *
     * @param job
     */
    private void submitToUserQueue( CCRSJob job ) {
        log.info( "Submitting job (" + job.getJobId() + ") for client-user: (" + userQueueKey(job) + ") to user queue" );

        Queue<CCRSJob> jobs = userQueues.computeIfAbsent( userQueueKey(job), k -> new LinkedList<>() );

        if ( jobs.size() > clientSettings.getClients().get( job.getClientId() ).getUserJobLimit() ) {
            log.info( "Too many jobs (" + job.getJobId() + ") for client-user: (" + userQueueKey(job) + ")");
            return;
        }

        synchronized ( jobs ) {

            if ( !jobs.contains( job ) ) {
                jobs.add( job );
                job.setStatus( "Pending" );
                saveJob( job );
                submitTopOfUserQueue( userQueueKey(job) );
            }
        }
    }

    /**
     * Begin process of submitting a job to the queueing system.
     *
     * @param job
     * @return Message for things like validation failure.
     */
    public String submit( CCRSJob job ) {

        if ( job.isComplete() || job.isFailed() || job.isRunning() ) {
            return "Job already submitted.";
        }

        if ( job.isEmailOnJobSubmitted() && job.getEmail() != null && !job.getEmail().isEmpty() ) {
            try {
                emailService.sendJobSubmittedMessage( job );
            } catch ( MessagingException | MailSendException e ) {
                log.warn( e );
            }
        }

        submitToUserQueue( job );

        return "";
    }

    public CCRSJob getSavedJob( String jobId ) {
        CCRSJob job = savedJobs.get( jobId );
        if ( job !=null ) {
            // Reset purge datetime
            job.setSaveExpiredDate( System.currentTimeMillis() + applicationSettings.getPurgeAfterHours() * 60 * 60 * 1000 );
        }
        return job;
    }

    public void saveJob( CCRSJob job ) {
        synchronized ( savedJobs ) {
            job.setSaved( true );
            savedJobs.put( job.getJobId(), job );
        }
    }

    private void removeSavedJob( CCRSJob job ) {
        synchronized ( savedJobs ) {
            savedJobs.remove( job.getJobId() );
        }

        // Delete serialization on disk
        try {
            Path serializedJob = job.getJobsDirectory().resolve( job.getJobSerializationFilename() );
            Files.deleteIfExists( serializedJob );
        } catch ( IOException e ) {
            log.error(e);
        }
    }


    public void stopJob( CCRSJob job ) {
        log.info( "Requesting job stop (" + job.getJobId() + ") for client-user: (" + userQueueKey(job) + ")" );
        // Start at the lowest queue and work our way up to minimize race conditions

        Queue<CCRSJob> jobs = userQueues.get( userQueueKey(job) );
        if ( jobs != null ) {
            synchronized ( jobs ) {
                if ( jobs.contains( job ) ) {
                    // Not yet submitted, just remove it from user queue
                    jobs.remove( job );
                }
            }
        }

        jobs = clientQueues.get( job.getClientId() );
        if ( jobs != null ) {
            synchronized ( jobs ) {
                if ( jobs.contains( job ) ) {
                    // Not yet submitted, just remove it from client queue
                    jobs.remove( job );
                }
            }
        }

        synchronized ( jobQueueMirror ) {
            if ( jobQueueMirror.contains( job ) ) {
                if ( job.isComplete() ) {
                    jobQueueMirror.remove( job );
                } else if ( job.isRunning() ) {
                    // Disabled for now as it doesn't currently work if job is running
//                    job.getFuture().cancel( true );
//                    jobQueueMirror.remove( job );
                } else {
                    job.getFuture().cancel( true );
                    jobQueueMirror.remove( job );
                }
            }
        }

        // Remove the job from saved cache no matter what so that it becomes inaccessible
        removeSavedJob( job );

        // Fix any gaps created in the queues, if there are no gaps these do nothing so safe to run in either case
        submitTopOfClientQueue( job.getClientId() );
        submitTopOfUserQueue( userQueueKey( job ) );
    }

    public void onJobStart( CCRSJob job ) {
        if ( job.isEmailOnJobStart() && job.getEmail() != null && !job.getEmail().isEmpty() ) {
            try {
                emailService.sendJobStartMessage( job );
            } catch ( MessagingException | MailSendException e ) {
                log.warn( e );
            }
        }

        // Update positions
        synchronized ( jobQueueMirror ) {
            int idx = 1;
            for ( CCRSJob ccrsJob : jobQueueMirror ) {
                if ( !ccrsJob.isRunning() ) {
                    ccrsJob.setPosition( idx );
                    ccrsJob.setStatus( "Position: " + idx );
                    idx++;
                }
            }
        }
    }

    public void onJobComplete( CCRSJob job ) {
        job.setSaveExpiredDate( System.currentTimeMillis() + applicationSettings.getPurgeAfterHours() * 60 * 60 * 1000 );
        if ( job.isEmailOnJobComplete() && job.getEmail() != null && !job.getEmail().isEmpty() ) {
            try {
                emailService.sendJobCompletionMessage( job );
            } catch ( MessagingException | MailSendException e ) {
                log.warn( e );
            }
        }
        // Remove job from queue mirror
        job.setPosition( null );
        synchronized ( jobQueueMirror ) {
            jobQueueMirror.remove( job );
        }

        // Increment counts
        completionCounts.putIfAbsent( job.getClientId(), new AtomicInteger( 0 ) );
        completionCounts.get( job.getClientId() ).incrementAndGet();

        // Add new job for given client
        submitTopOfClientQueue( job.getClientId() );
        submitTopOfUserQueue( userQueueKey( job ) );
        log.info( String.format( "Jobs in queue: %d", jobQueueMirror.size() ) );
    }

    public int getCompletionCount(String clientId) {
        AtomicInteger count = completionCounts.get( clientId );
        return count == null ? 0 : count.get();
    }

    public List<CCRSJob.CCRSJobVO> listPublicJobs() {
        return Stream.concat(jobQueueMirror.stream(), savedJobs.values().stream())
                .distinct()
                .filter( j -> !j.isHidden() )
                .map( j -> j.toValueObject( true ) )
                .sorted(
                        Comparator.comparing(CCRSJob.CCRSJobVO::isComplete, Comparator.nullsLast(Boolean::compareTo))
                                .thenComparing(CCRSJob.CCRSJobVO::getPosition, Comparator.nullsLast(Integer::compareTo))
                                .thenComparing(CCRSJob.CCRSJobVO::getSubmittedDate, Comparator.nullsLast(Date::compareTo).reversed())
                                .thenComparing(CCRSJob.CCRSJobVO::getStatus, String::compareToIgnoreCase)
                )
                .collect( Collectors.toList() );
    }

    public List<CCRSJob.CCRSJobVO> listJobsForClient(String clientId) {
        return Stream.concat(jobQueueMirror.stream(), savedJobs.values().stream())
                .distinct()
                .filter( j -> j.getClientId().equals( clientId ) )
                .map( j -> j.toValueObject( true ) )
                .sorted(
                        Comparator.comparing(CCRSJob.CCRSJobVO::isComplete, Comparator.nullsLast(Boolean::compareTo))
                                .thenComparing(CCRSJob.CCRSJobVO::getPosition, Comparator.nullsLast(Integer::compareTo))
                                .thenComparing(CCRSJob.CCRSJobVO::getSubmittedDate, Comparator.nullsLast(Date::compareTo).reversed())
                                .thenComparing(CCRSJob.CCRSJobVO::getStatus, String::compareToIgnoreCase)
                )
                .collect( Collectors.toList() );
    }

    public List<CCRSJob.CCRSJobVO> listJobsForClientAndUser(String clientId, String userId) {
        return Stream.concat(jobQueueMirror.stream(), savedJobs.values().stream())
                .distinct()
                .filter( j -> j.getClientId().equals( clientId ) && j.getUserId().equals( userId ) )
                .map( j -> j.toValueObject( true ) )
                .sorted(
                        Comparator.comparing(CCRSJob.CCRSJobVO::isComplete, Comparator.nullsLast(Boolean::compareTo))
                                .thenComparing(CCRSJob.CCRSJobVO::getPosition, Comparator.nullsLast(Integer::compareTo))
                                .thenComparing(CCRSJob.CCRSJobVO::getSubmittedDate, Comparator.nullsLast(Date::compareTo).reversed())
                                .thenComparing(CCRSJob.CCRSJobVO::getStatus, String::compareToIgnoreCase)
                )
                .collect( Collectors.toList() );
    }



}
