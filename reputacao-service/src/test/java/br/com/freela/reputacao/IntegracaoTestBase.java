package br.com.freela.reputacao;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "management.tracing.export.enabled=false",
        "app.kafka.retry.intervalo-inicial-ms=100"
})
@Import(IntegracaoTestBase.Topicos.class)
public abstract class IntegracaoTestBase {

    protected static final String TOPICO = "contratos.eventos.v1";
    protected static final String TOPICO_DLT = "contratos.eventos.v1.DLT";

    @ServiceConnection
    protected static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.2.1");

    @ServiceConnection
    protected static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16");

    static {
        KAFKA.start();
        POSTGRES.start();
    }

    @Autowired
    protected KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    protected JdbcTemplate jdbc;

    @TestConfiguration
    static class Topicos {

        @Bean
        NewTopic contratos() {
            return TopicBuilder.name(TOPICO).partitions(3).replicas(1).build();
        }

        @Bean
        NewTopic contratosDlt() {
            return TopicBuilder.name(TOPICO_DLT).partitions(3).replicas(1).build();
        }
    }

    protected void publicar(UUID eventId, String eventType, UUID contratoId, String payload) {
        String envelope = """
                {"eventId":"%s","eventType":"%s","eventVersion":1,"contratoId":"%s","occurredAt":"%s",\
                "correlationId":"teste-%s","producer":"teste","payload":%s}"""
                .formatted(eventId, eventType, contratoId, Instant.now(), eventType, payload);
        publicarMensagem(contratoId.toString(), eventId.toString(), envelope);
    }

    protected void publicarMensagem(String key, String eventId, String value) {
        var record = new ProducerRecord<String, String>(TOPICO, key, value);
        record.headers().add(new RecordHeader("eventId", eventId.getBytes(StandardCharsets.UTF_8)));
        try {
            kafkaTemplate.send(record).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("falha ao publicar mensagem de teste", e);
        }
    }

    protected String payload(UUID contratoId, UUID clienteId, UUID freelancerId, BigDecimal valor) {
        return """
                {"contratoId":"%s","clienteId":"%s","freelancerId":"%s","valor":%s}"""
                .formatted(contratoId, clienteId, freelancerId, valor);
    }

    protected void aguardarProcessado(UUID eventId) {
        await().atMost(Duration.ofSeconds(30)).until(() -> jdbc.queryForObject(
                "select count(*) from eventos_processados where event_id = ?", Integer.class, eventId) == 1);
    }
}
