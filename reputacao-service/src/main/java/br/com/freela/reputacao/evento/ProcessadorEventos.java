package br.com.freela.reputacao.evento;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Consumer;

@Component
public class ProcessadorEventos {

    private static final Logger log = LoggerFactory.getLogger(ProcessadorEventos.class);

    private static final String MDC_CORRELATION_ID = "correlationId";
    private static final String MDC_CONTRATO_ID = "contratoId";
    private static final String MDC_EVENT_ID = "eventId";
    private static final String MDC_EVENT_TYPE = "eventType";
    private static final List<String> CHAVES_MDC = List.of(
            MDC_CORRELATION_ID,
            MDC_CONTRATO_ID,
            MDC_EVENT_ID,
            MDC_EVENT_TYPE
    );

    private final EventoContratoParser parser;
    private final IdempotenciaService idempotencia;
    private final TransactionTemplate transactionTemplate;

    public ProcessadorEventos(
            EventoContratoParser parser,
            IdempotenciaService idempotencia,
            TransactionTemplate transactionTemplate
    ) {
        this.parser = parser;
        this.idempotencia = idempotencia;
        this.transactionTemplate = transactionTemplate;
    }

    public boolean processar(ConsumerRecord<String, String> record, Consumer<EventoContrato> efeito) {
        colocarMdcDosHeaders(record);
        try {
            log.info("evento.recebido topic={} partition={} offset={} eventType={}",
                    record.topic(), record.partition(), record.offset(), MDC.get(MDC_EVENT_TYPE));

            EventoContrato evento = parser.parse(record.value());
            colocarMdcDoEvento(evento);

            log.info("evento.processamento.inicio eventType={}", evento.eventType());
            boolean aplicado = Boolean.TRUE.equals(transactionTemplate.execute(status -> aplicar(evento, efeito)));
            if (aplicado) {
                log.info("evento.processamento.sucesso eventType={}", evento.eventType());
            }
            return aplicado;
        } catch (RuntimeException e) {
            log.error("evento.processamento.falha topic={} partition={} offset={} causa={}",
                    record.topic(), record.partition(), record.offset(),
                    e.getClass().getSimpleName() + ": " + e.getMessage());
            throw e;
        } finally {
            CHAVES_MDC.forEach(MDC::remove);
        }
    }

    private boolean aplicar(EventoContrato evento, Consumer<EventoContrato> efeito) {
        if (!idempotencia.registrarSeNovo(evento)) {
            return false;
        }
        efeito.accept(evento);
        return true;
    }

    private void colocarMdcDosHeaders(ConsumerRecord<String, String> record) {
        colocarMdc(MDC_CORRELATION_ID, header(record, "correlationId"));
        colocarMdc(MDC_CONTRATO_ID, record.key());
        colocarMdc(MDC_EVENT_ID, header(record, "eventId"));
        colocarMdc(MDC_EVENT_TYPE, header(record, "eventType"));
    }

    private void colocarMdcDoEvento(EventoContrato evento) {
        colocarMdc(MDC_CORRELATION_ID, evento.correlationId());
        colocarMdc(MDC_CONTRATO_ID, evento.contratoId().toString());
        colocarMdc(MDC_EVENT_ID, evento.eventId().toString());
        colocarMdc(MDC_EVENT_TYPE, evento.eventType());
    }

    private void colocarMdc(String chave, String valor) {
        if (valor != null) {
            MDC.put(chave, valor);
        }
    }

    private String header(ConsumerRecord<String, String> record, String nome) {
        Header header = record.headers().lastHeader(nome);
        if (header == null || header.value() == null) {
            return null;
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
