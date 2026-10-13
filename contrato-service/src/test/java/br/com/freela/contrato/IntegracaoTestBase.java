package br.com.freela.contrato;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "management.tracing.export.enabled=false",
        "app.outbox.intervalo-ms=200"
})
public abstract class IntegracaoTestBase {

    @ServiceConnection
    protected static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.2.1");

    @ServiceConnection
    protected static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16");

    static {
        KAFKA.start();
        POSTGRES.start();
    }
}
