package com.github.jeremylford.spring.kafkaconnect;

import org.apache.kafka.connect.runtime.WorkerConfig;
import org.springframework.boot.autoconfigure.jersey.JerseyProperties;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.web.util.UriBuilder;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Locale;

public class AdvertisedURL {


    private static final String PROTOCOL_HTTP = "http";
    private static final String PROTOCOL_HTTPS = "https";

    private final JerseyProperties jerseyProperties;
    private final ServerProperties serverProperties;
    private final WorkerConfig workerConfig;

    public AdvertisedURL(JerseyProperties jerseyProperties, ServerProperties serverProperties, WorkerConfig workerConfig) {
        this.jerseyProperties = jerseyProperties;
        this.serverProperties = serverProperties;
        this.workerConfig = workerConfig;
    }

    public URI get() {
        String advertisedSecurityProtocol = determineAdvertisedProtocol(workerConfig);
        Integer advertisedPort = determineAdvertisedPort(workerConfig, serverProperties);
        String advertisedHostName = determineAdvertisedHostName(workerConfig, serverProperties);

        UriBuilder uriBuilder = UriComponentsBuilder.newInstance()
                .scheme(advertisedSecurityProtocol)
                .host(advertisedHostName)
                .port(advertisedPort);

        String path = jerseyProperties.getApplicationPath();
        if (path != null) {
            uriBuilder = uriBuilder.path(path);
        }

        return uriBuilder.build();
    }

    private static Integer determineAdvertisedPort(WorkerConfig config, ServerProperties serverProperties) {
        Integer advertisedPort = config.getInt(WorkerConfig.REST_ADVERTISED_PORT_CONFIG);
        if (advertisedPort == null) {
            advertisedPort = serverProperties.getPort();
            if (advertisedPort == null) {
                advertisedPort = 8080; //standard default of springboot
            }
        }
        return advertisedPort;
    }

    private static String determineAdvertisedHostName(WorkerConfig config, ServerProperties serverProperties) {
        String advertisedHostName = config.getString(WorkerConfig.REST_ADVERTISED_HOST_NAME_CONFIG);
        if (advertisedHostName == null) {
            InetAddress address = serverProperties.getAddress();
            if (address != null) {
                advertisedHostName = address.getHostName();
            } else {
                try {
                    advertisedHostName = InetAddress.getLocalHost().getHostAddress();
                } catch (UnknownHostException e) {
                    advertisedHostName = "localhost";
                }
            }
        }
        return advertisedHostName;
    }

    /**
     * Original implementation can be found at
     * {@link org.apache.kafka.connect.runtime.rest.RestServer#determineAdvertisedProtocol}
     */
    private static String determineAdvertisedProtocol(WorkerConfig config) {
        String advertisedSecurityProtocol = config.getString(WorkerConfig.REST_ADVERTISED_LISTENER_CONFIG);
        if (advertisedSecurityProtocol == null) {
            String listeners = (String) config.originals().get(WorkerConfig.LISTENERS_CONFIG);

            if (listeners == null) {
                return PROTOCOL_HTTP;
            } else {
                listeners = listeners.toLowerCase(Locale.ENGLISH);
            }

            if (listeners.contains(String.format("%s://", PROTOCOL_HTTP))) {
                return PROTOCOL_HTTP;
            } else if (listeners.contains(String.format("%s://", PROTOCOL_HTTPS))) {
                return PROTOCOL_HTTPS;
            } else {
                return PROTOCOL_HTTP;
            }
        } else {
            return advertisedSecurityProtocol.toLowerCase(Locale.ENGLISH);
        }
    }
}
