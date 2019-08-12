package com.jacobsonmt.ccrs.repositories;

import com.jacobsonmt.ccrs.model.CCRSJob;

import java.util.stream.Stream;

public interface JobRepository {

    CCRSJob getById( String id );

    String getRawResultFileById( String id );

    Stream<CCRSJob> allJobsForClientAndUser( String clientId, String userId);

    void delete( CCRSJob job );

    void persistJob( CCRSJob job );

    void cacheJob( CCRSJob job );

}
