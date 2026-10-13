package br.com.freela.contrato.infrastructure.messaging.outbox;

import br.com.freela.contrato.infrastructure.correlation.CorrelationId;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final SpringDataOutboxEventoRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topico;
    private final int tamanhoLote;
    private final long timeoutEnvioMs;
    private final long chaveLock;
    private final Tracer tracer;
    private final Propagator propagator;

    public OutboxPublisher(
            SpringDataOutboxEventoRepository repository,
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${app.kafka.topics.contratos}") String topico,
            @Value("${app.outbox.lote}") int tamanhoLote,
            @Value("${app.outbox.timeout-envio-ms}") long timeoutEnvioMs,
            @Value("${app.outbox.chave-lock}") long chaveLock,
            Tracer tracer,
            Propagator propagator
    ) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.topico = topico;
        this.tamanhoLote = tamanhoLote;
        this.timeoutEnvioMs = timeoutEnvioMs;
        this.chaveLock = chaveLock;
        this.tracer = tracer;
        this.propagator = propagator;
    }

    @Scheduled(fixedDelayString = "${app.outbox.intervalo-ms}")
    @Transactional
    public void publicarPendentes() {
        if (!repository.tentarLockPublicacao(chaveLock)) {
            return;
        }

        List<OutboxEventoJpaEntity> pendentes = repository.buscarPendentes(tamanhoLote);
        if (pendentes.isEmpty()) {
            return;
        }

        log.info("contrato.outbox.lote.inicio quantidade={}", pendentes.size());
        int publicados = 0;
        for (OutboxEventoJpaEntity evento : pendentes) {
            if (!publicar(evento)) {
                break;
            }
            publicados++;
        }
        log.info("contrato.outbox.lote.fim publicados={} pendentesNoLote={}", publicados, pendentes.size() - publicados);
    }

    private boolean publicar(OutboxEventoJpaEntity evento) {
        Span span = iniciarSpan(evento);
        MDC.put(CorrelationId.MDC_KEY, evento.getCorrelationId());
        try (Tracer.SpanInScope escopo = tracer.withSpan(span)) {
            SendResult<String, String> resultado = kafkaTemplate.send(criarRecord(evento))
                    .get(timeoutEnvioMs, TimeUnit.MILLISECONDS);
            evento.marcarPublicado(Instant.now());

            var metadata = resultado.getRecordMetadata();
            log.info("contrato.evento.publicado contratoId={} eventId={} eventType={} topic={} partition={} offset={}",
                    evento.getContratoId(), evento.getEventId(), evento.getEventType(),
                    metadata.topic(), metadata.partition(), metadata.offset());
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            span.error(e);
            registrarFalha(evento, e);
            return false;
        } catch (Exception e) {
            span.error(e);
            registrarFalha(evento, e);
            return false;
        } finally {
            span.end();
            MDC.remove(CorrelationId.MDC_KEY);
        }
    }

    private Span iniciarSpan(OutboxEventoJpaEntity evento) {
        Map<String, String> carrier = evento.getTraceContext() == null
                ? Map.of()
                : Map.of(OutboxEventoAdapter.TRACEPARENT, evento.getTraceContext());
        return propagator.extract(carrier, (mapa, chave) -> mapa.get(chave))
                .name("outbox.publish")
                .tag("eventType", evento.getEventType())
                .tag("eventId", evento.getEventId().toString())
                .start();
    }

    private ProducerRecord<String, String> criarRecord(OutboxEventoJpaEntity evento) {
        var record = new ProducerRecord<String, String>(topico, evento.getContratoId().toString(), evento.getPayload());
        record.headers()
                .add(header("eventId", evento.getEventId().toString()))
                .add(header("eventType", evento.getEventType()))
                .add(header("correlationId", evento.getCorrelationId()));
        return record;
    }

    private RecordHeader header(String nome, String valor) {
        return new RecordHeader(nome, valor.getBytes(StandardCharsets.UTF_8));
    }

    private void registrarFalha(OutboxEventoJpaEntity evento, Throwable erro) {
        Throwable causa = causaRaiz(erro);
        String mensagem = causa.getClass().getSimpleName() + ": " + causa.getMessage();
        evento.registrarFalha(mensagem);
        log.error("contrato.evento.publicacao.falha contratoId={} eventId={} eventType={} tentativas={} causa={}",
                evento.getContratoId(), evento.getEventId(), evento.getEventType(), evento.getTentativas(), mensagem);
    }

    private Throwable causaRaiz(Throwable erro) {
        Throwable atual = erro;
        while (atual.getCause() != null && atual.getCause() != atual) {
            atual = atual.getCause();
        }
        return atual;
    }
}
