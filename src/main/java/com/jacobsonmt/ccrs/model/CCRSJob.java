package com.jacobsonmt.ccrs.model;

import com.jacobsonmt.ccrs.exceptions.ResultFileException;
import com.jacobsonmt.ccrs.services.JobManager;
import lombok.*;
import lombok.extern.log4j.Log4j2;
import org.springframework.util.StopWatch;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

@Log4j2
@Getter
@Setter
@Builder
@ToString(of = {"jobId", "clientId", "userId", "label", "hidden"})
@EqualsAndHashCode(of = {"jobId"})
public class CCRSJob implements Callable<CCRSJobResult>, Serializable {

    private static final long serialVersionUID = 1L;

    // Path to resources
    private transient String command;
    private transient Path jobsDirectory;
    private String outputCSVFilename;
    private String inputFASTAFilename;
    private String jobSerializationFilename;

    // Information on creation of job
    private String clientId;
    private String userId;
    private String jobId;
    private String label;
    private transient String inputFASTAContent;
    @Builder.Default private boolean hidden = true;
    private Date submittedDate;
    private Date startedDate;
    private Date finishedDate;
    private transient String email;
    private transient String emailJobLinkPrefix; // TODO: Awkward, but good enough for now
    @Builder.Default private transient boolean emailOnJobSubmitted = false;
    @Builder.Default private transient boolean emailOnJobStart= false;
    @Builder.Default private transient boolean emailOnJobComplete= true;

    // Information on running / completion
    @Builder.Default private boolean running = false;
    @Builder.Default private boolean failed = false;
    @Builder.Default private boolean complete = false;
    private transient Integer position;
    private String status;

    // Results
    private transient CCRSJobResult result;
    private long executionTime;

    // Saving Job information / results for later
    @Builder.Default private transient boolean saved = false;
    private transient Long saveExpiredDate;

    // Back-reference to owning JobManager
    private transient JobManager jobManager;

    // Used to cancel job
    private transient Future<CCRSJobResult> future;

    @Override
    public CCRSJobResult call() throws Exception {

        try {

            log.info( "Starting job (" + label + ") for client: (" + clientId + ")" );

            this.running = true;
            this.status = "Processing";
            this.position = 0;
            this.startedDate =  new Date();

            jobManager.onJobStart( this );

            // Create job directory
            Files.createDirectories( jobsDirectory );

            // Write content to input

            Path fastaFile = jobsDirectory.resolve( inputFASTAFilename );
            try ( BufferedWriter writer = Files.newBufferedWriter( fastaFile, Charset.forName( "UTF-8" ) ) ) {
                writer.write( inputFASTAContent );
            }

            // Execute script
            StopWatch sw = new StopWatch();
            sw.start();
            String[] commands = {command, inputFASTAFilename};
            String output = executeCommand( commands, jobsDirectory );
            log.debug( output );
            sw.stop();
            this.executionTime = sw.getTotalTimeMillis() / 1000;
            this.finishedDate =  new Date();

            // Get output
            this.result = CCRSJobResult.parseResultCSVStream( Files.newInputStream( jobsDirectory.resolve( outputCSVFilename ) ) );
            if ( this.result.getTaxa().getKey().equals( Taxa.KnownKeyTypes.OX.name() ) ) {
                this.status = "Completed in " + executionTime + "s";
            } else if ( this.result.getTaxa().getKey().equals( Taxa.KnownKeyTypes.malformed_OX.name() ) ||
                    this.result.getTaxa().getKey().equals( Taxa.KnownKeyTypes.missing_OX.name() )) {
                // FIXME: Improve this
                log.warn( "Unexpected Taxa Line Key: {}", this.result.getTaxa().getKey() );
                throw new ResultFileException( "Unexpected Error - Failed To Process" );
            } else {
                throw new ResultFileException( this.result.getTaxa().getKey() );
            }

            log.info( "Finished job (" + label + ") for client: (" + clientId + ")" );
            this.running = false;
            this.complete = true;

        } catch ( ResultFileException e ) {
            log.error( e );
            fail( e.getMessage() );
        } catch ( Exception e ) {
            log.error( e );
            fail( "Failed after " + executionTime + "s" );
        }

        jobManager.onJobComplete( this );
        jobManager = null;
        return this.result;

    }

    private void fail(String status) {
        this.finishedDate =  new Date();
        this.result = CCRSJobResult.createNullResult();
        this.complete = true;
        this.running = false;
        this.failed = true;
        this.status = status;
    }

    private static String executeCommand( String[] command, Path path ) {

        StringBuffer output = new StringBuffer();

        try {
            ProcessBuilder builder = new ProcessBuilder( command )
                    .directory( path.toFile() );
//            builder.redirectErrorStream();


//            Process p = Runtime.getRuntime().exec( command, null, path.toFile() );
            Process p = builder.start();

            BufferedReader reader = new BufferedReader( new InputStreamReader( p.getInputStream() ) );

            String line = "";
            while ( ( line = reader.readLine() ) != null ) {
                output.append( line + "\r\n" );
                log.debug(line);
            }

            reader = new BufferedReader( new InputStreamReader( p.getErrorStream() ) );

            while ( ( line = reader.readLine() ) != null ) {
                output.append( line + "\r\n" );
                log.debug(line);
            }


            p.waitFor();
//            reader.close();

        } catch ( Exception e ) {
            log.error(e);
        }

        return output.toString();
    }

    public static String inputStreamToString(InputStream inputStream) throws IOException {
        StringBuilder textBuilder = new StringBuilder();
        try (Reader reader = new BufferedReader(new InputStreamReader
                (inputStream, Charset.forName(StandardCharsets.UTF_8.name())))) {
            int c = 0;
            while ((c = reader.read()) != -1) {
                textBuilder.append((char) c);
            }
        }
        return textBuilder.toString();
    }

    @Getter
    @AllArgsConstructor
    public static final class CCRSJobVO {
        private final String jobId;
        private final String clientId;
        private final String label;
        private final String status;
        private final boolean running;
        private final boolean failed;
        private final boolean complete;
        private final Integer position;
        private final String email;
        private final boolean hidden;
        private final Date submittedDate;
        private final Date startedDate;
        private final Date finishedDate;
        private final String inputFASTAContent;
        private final CCRSJobResult result;
        private final long executionTime;
    }

    public static String obfuscateEmail( String email ) {
        if ( email == null ) {
            return null;
        }
        return email.replaceAll( "(\\w{0,3})(\\w+.*)(@.*)", "$1****$3" );
    }

    public CCRSJobVO toValueObject( boolean obfuscateEmail, boolean withResults) {
        return new CCRSJobVO( jobId, clientId, label, status, running, failed, complete, position,
                obfuscateEmail ? obfuscateEmail(email) : email,
                hidden, submittedDate, startedDate, finishedDate, inputFASTAContent,
                !withResults && result != null ? CCRSJobResult.createWithOnlyTaxa( result.getTaxa() ) : result,
                executionTime );
    }

}
