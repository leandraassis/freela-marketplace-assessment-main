package br.com.freela.contrato;

import br.com.freela.contrato.application.ContratoApplicationService;
import br.com.freela.contrato.application.CriarContratoCommand;
import br.com.freela.contrato.infrastructure.correlation.CorrelationId;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class OutboxPublisherIntegrationTest extends IntegracaoTestBase {

    private static final String TOPICO = "contratos.eventos.v1";

    @Autowired
    private ContratoApplicationService service;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private JsonMapper jsonMapper;

    @AfterEach
    void limparMdc() {
        MDC.remove(CorrelationId.MDC_KEY);
    }

    @Test
    void publicaEventosDoContratoComKeyHeadersENaOrdem() {
        MDC.put(CorrelationId.MDC_KEY, "teste-publicacao");
        UUID contratoId = service.criar(new CriarContratoCommand(
                UUID.randomUUID(), UUID.randomUUID(), "Teste publicacao", new BigDecimal("250.00"))).id();
        service.registrarEntrega(contratoId);
        service.concluir(contratoId);

        List<ConsumerRecord<String, String>> registros = consumirDoContrato(contratoId, 3);

        assertThat(registros).extracting(r -> header(r, "eventType"))
                .containsExactly("ContratoCriado", "EntregaRegistrada", "ContratoConcluido");
        assertThat(registros).allSatisfy(r -> {
            assertThat(r.key()).isEqualTo(contratoId.toString());
            assertThat(r.partition()).isEqualTo(registros.get(0).partition());
            assertThat(header(r, "correlationId")).isEqualTo("teste-publicacao");

            JsonNode envelope = jsonMapper.readTree(r.value());
            assertThat(header(r, "eventId")).isEqualTo(envelope.get("eventId").asString());
            assertThat(envelope.get("contratoId").asString()).isEqualTo(contratoId.toString());
        });
        assertThat(registros).extracting(ConsumerRecord::offset).isSorted();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(jdbc.queryForObject(
                "select count(*) from outbox_eventos where contrato_id = ? and status = 'PUBLICADO'",
                Integer.class, contratoId)).isEqualTo(3));
    }

    private List<ConsumerRecord<String, String>> consumirDoContrato(UUID contratoId, int quantidade) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "teste-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        );
        List<ConsumerRecord<String, String>> registros = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(TOPICO));
            await().atMost(Duration.ofSeconds(30)).until(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(r -> {
                    if (contratoId.toString().equals(r.key())) {
                        registros.add(r);
                    }
                });
                return registros.size() >= quantidade;
            });
        }
        return registros;
    }

    private String header(ConsumerRecord<String, String> registro, String nome) {
        return new String(registro.headers().lastHeader(nome).value(), StandardCharsets.UTF_8);
    }
}
