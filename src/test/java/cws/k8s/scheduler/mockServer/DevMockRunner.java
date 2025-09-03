package cws.k8s.scheduler.mockServer;

import cws.k8s.scheduler.Main;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import org.springframework.boot.SpringApplication;

/**
 * Entwicklungs-Starter: Startet Mock-Kubernetes + deinen Scheduler.
 */
public class DevMockRunner {

    private static KubernetesServer server;

    public static void main(String[] args) {
        if( System.getenv( "MODE" ) == null || System.getenv( "MODE" ).isEmpty() ){
            throw new IllegalArgumentException( "Please define environment variable: MODE; (mock/real)" );
        }
        SpringApplication app = new SpringApplication(Main.class);
        app.addListeners(event -> {
            if (event instanceof org.springframework.boot.web.context.WebServerInitializedEvent e) {
                System.out.println(">>> Embedded Server running on port: " + e.getWebServer().getPort());
            }
        });
        app.run(args);

        System.out.println("Dev Mock Scheduler läuft. REST-Endpunkte jetzt aufrufen.");
    }
}