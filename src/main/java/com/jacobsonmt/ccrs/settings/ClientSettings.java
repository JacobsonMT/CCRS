package com.jacobsonmt.ccrs.settings;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@ConfigurationProperties(prefix = "ccrs")
@Getter
@Setter
public class ClientSettings {

    private Map<String, ApplicationClient> clients = new ConcurrentHashMap<>();

    @Getter
    @Setter
    @ToString
    public static class ApplicationClient {

        private String name;
        private String token;
        private int processLimit = 2;
        private int jobLimit = 100;

    }
}