package cws.k8s.scheduler.config;

import cws.k8s.scheduler.client.CWSKubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import io.fabric8.kubernetes.client.KubernetesClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Kürzere Variante mit KubernetesServer (stellt Server + Client bereit).
 */
@Slf4j
@Configuration
public class Beans {

    @Bean
    public KubernetesServer kubernetesServer() {
        // Parameter: (https, crud)
        KubernetesServer server = new KubernetesServer(true, true);
        server.before(); // startet den Server (bei JUnit wäre das automatisch)
        return server;
    }

    @Bean
    public KubernetesClient fabric8MockClient(KubernetesServer server) {
        var client = server.getClient();
        log.info("[MOCK] Fabric8 Client via KubernetesServer. URL={}", client.getConfiguration().getMasterUrl());
        return client;
    }

    @Bean
    public CWSKubernetesClient cwsKubernetesClient(KubernetesClient fabric8MockClient) {
        return new CWSKubernetesClient(fabric8MockClient);
    }
}