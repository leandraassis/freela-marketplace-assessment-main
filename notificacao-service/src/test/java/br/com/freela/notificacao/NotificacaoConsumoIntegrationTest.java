package br.com.freela.notificacao;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class NotificacaoConsumoIntegrationTest extends IntegracaoTestBase {

    private final UUID clienteId = UUID.randomUUID();
    private final UUID freelancerId = UUID.randomUUID();

    @Test
    void contratoCriadoNotificaOFreelancer() {
        UUID eventId = UUID.randomUUID();
        UUID contratoId = UUID.randomUUID();

        publicar(eventId, "ContratoCriado", contratoId, payloadDe(contratoId));
        aguardarProcessado(eventId);

        Map<String, Object> notificacao = jdbc.queryForMap(
                "select destinatario_id, tipo from notificacoes where event_id = ?", eventId);
        assertThat(notificacao.get("destinatario_id")).isEqualTo(freelancerId);
        assertThat(notificacao.get("tipo")).isEqualTo("CONTRATO_CRIADO");
    }

    @Test
    void contratoCanceladoNotificaClienteEFreelancer() {
        UUID eventId = UUID.randomUUID();
        UUID contratoId = UUID.randomUUID();

        publicar(eventId, "ContratoCancelado", contratoId, payloadDe(contratoId));
        aguardarProcessado(eventId);

        assertThat(jdbc.queryForList(
                "select destinatario_id from notificacoes where event_id = ?", UUID.class, eventId))
                .containsExactlyInAnyOrder(clienteId, freelancerId);
    }

    @Test
    void mensagemDuplicadaNaoGeraNotificacaoRepetida() {
        UUID eventId = UUID.randomUUID();
        UUID marcador = UUID.randomUUID();
        UUID contratoId = UUID.randomUUID();

        publicar(eventId, "ContratoCriado", contratoId, payloadDe(contratoId));
        publicar(eventId, "ContratoCriado", contratoId, payloadDe(contratoId));
        publicar(marcador, "EntregaRegistrada", contratoId, payloadDe(contratoId));
        aguardarProcessado(marcador);

        assertThat(jdbc.queryForObject(
                "select count(*) from notificacoes where event_id = ?", Integer.class, eventId))
                .isEqualTo(1);
    }

    @Test
    void falhaNoProcessamentoEnviaMensagemParaDltAposAsTentativas() {
        UUID eventId = UUID.randomUUID();
        UUID contratoId = UUID.fromString(CONTRATO_COM_FALHA);

        publicar(eventId, "ContratoCriado", contratoId, payloadDe(contratoId));

        ConsumerRecord<String, String> naDlt = aguardarNaDlt(contratoId.toString());
        assertThat(naDlt.value()).contains(eventId.toString());
        assertThat(header(naDlt, "kafka_dlt-original-topic")).isEqualTo(TOPICO);
        assertThat(header(naDlt, "kafka_dlt-exception-cause-fqcn")).isEqualTo(IllegalStateException.class.getName());
        assertThat(jdbc.queryForObject(
                "select count(*) from notificacoes where event_id = ?", Integer.class, eventId))
                .isZero();
    }

    @Test
    void mensagemInvalidaVaiDiretoParaDlt() {
        String key = UUID.randomUUID().toString();

        publicarMensagem(key, UUID.randomUUID().toString(), "{nao e json");

        ConsumerRecord<String, String> naDlt = aguardarNaDlt(key);
        assertThat(naDlt.value()).isEqualTo("{nao e json");
        assertThat(header(naDlt, "kafka_dlt-exception-cause-fqcn")).endsWith("StreamReadException");
    }

    private ConsumerRecord<String, String> aguardarNaDlt(String key) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "teste-dlt-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        );
        AtomicReference<ConsumerRecord<String, String>> encontrado = new AtomicReference<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(TOPICO_DLT));
            await().atMost(Duration.ofSeconds(30)).until(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(r -> {
                    if (key.equals(r.key())) {
                        encontrado.set(r);
                    }
                });
                return encontrado.get() != null;
            });
        }
        return encontrado.get();
    }

    private String header(ConsumerRecord<String, String> registro, String nome) {
        return new String(registro.headers().lastHeader(nome).value(), StandardCharsets.UTF_8);
    }

    private String payloadDe(UUID contratoId) {
        return payload(contratoId, clienteId, freelancerId, new BigDecimal("100.00"));
    }
}
