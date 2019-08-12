package com.jacobsonmt.ccrs.repositories;

import com.jacobsonmt.ccrs.exceptions.ResultFileException;
import com.jacobsonmt.ccrs.model.CCRSJob;
import com.jacobsonmt.ccrs.model.CCRSJobResult;
import com.jacobsonmt.ccrs.settings.ApplicationSettings;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static com.jacobsonmt.ccrs.model.CCRSJob.inputStreamToString;

@Log4j2
@Primary
@Component
public class InMemoryJobRepository implements JobRepository {

    ApplicationSettings applicationSettings;

    private final Map<String, CCRSJob> savedJobs = new ConcurrentHashMap<>();

    @Autowired
    public InMemoryJobRepository( ApplicationSettings applicationSettings ) {
        this.applicationSettings = applicationSettings;

        if ( applicationSettings.isLoadJobsFromDisk() ) {
            loadJobsFromDisk();
        }

    }

    private void loadJobsFromDisk() {
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

                            try {
                                job.setResult( CCRSJobResult.parseResultCSVStream(
                                        Files.newInputStream( job.getJobsDirectory().resolve( job.getOutputCSVFilename() ) ) ) );
                            } catch ( ResultFileException e ) {
                                job.setResult( CCRSJobResult.createNullResult() );
                            }

                            job.setSaveExpiredDate( System.currentTimeMillis() + applicationSettings.getPurgeAfterHours() * 60 * 60 * 1000 );

                            cacheJob( job );
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

    @Override
    public CCRSJob getById( String id ) {
        CCRSJob job = savedJobs.get( id );
        if ( job !=null ) {
            // Reset purge datetime
            job.setSaveExpiredDate( System.currentTimeMillis() + applicationSettings.getPurgeAfterHours() * 60 * 60 * 1000 );
        }
        return job;
    }

    @Override
    public String getRawResultFileById( String id ) {
        CCRSJob job = savedJobs.get( id );
        if (job != null && job.getResult() != null ) {
            return job.getResult().getResultCSV();
        }
        return null;
    }

    @Override
    public Stream<CCRSJob> allJobsForClientAndUser( String clientId, String userId ) {
        return savedJobs.values().stream().filter( j -> j.getClientId().equals( clientId ) && j.getUserId().equals( userId ) );
    }

    @Override
    public void delete( CCRSJob job ) {
        savedJobs.remove( job.getJobId() );
        // Delete serialization on disk
        try {
            Path serializedJob = job.getJobsDirectory().resolve( job.getJobSerializationFilename() );
            Files.deleteIfExists( serializedJob );
        } catch ( IOException e ) {
            log.error(e);
        }
    }

    @Override
    public void persistJob( CCRSJob job ) {
        // Write metadata to job folder
        Path serializedJob = job.getJobsDirectory().resolve( job.getJobSerializationFilename() );
        try ( ObjectOutputStream oos = new ObjectOutputStream( Files.newOutputStream( serializedJob ) ) ) {
            oos.writeObject( job );
        } catch ( IOException e ) {
            log.error( "Failed to serialize job: " + job.getJobId(), e );
        }
    }

    @Override
    public void cacheJob( CCRSJob job ) {
        savedJobs.put( job.getJobId(), job );
    }
}
