package br.com.freela.contrato.infrastructure.messaging.outbox;

import br.com.freela.contrato.application.EventoOutbox;
import br.com.freela.contrato.domain.shared.DomainEvent;
import br.com.freela.contrato.infrastructure.correlation.CorrelationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import java.util.UUID;

@Component
public class OutboxEventoAdapter implements EventoOutbox {

    private static final Logger log = LoggerFactory.getLogger(OutboxEventoAdapter.class);

    private final SpringDataOutboxEventoRepository repository;
    private final EventoEnvelopeMapper mapper;

    public OutboxEventoAdapter(SpringDataOutboxEventoRepository repository, EventoEnvelopeMapper mapper) {
        this.repository = repository;
        this.mapper = mapper;
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void registrar(DomainEvent evento) {
        String correlationId = CorrelationId.atual().orElseGet(() -> gerarCorrelationId(evento));

        var outboxEvento = OutboxEventoJpaEntity.pendente(
                evento.eventId(),
                evento.eventType(),
                evento.contratoId(),
                correlationId,
                mapper.toJson(evento, correlationId),
                evento.occurredAt()
        );
        var registro = repository.save(outboxEvento);

        log.info("contrato.outbox.registrado outboxId={} contratoId={} eventId={} eventType={}",
                registro.getId(), evento.contratoId(), evento.eventId(), evento.eventType());
    }

    private String gerarCorrelationId(DomainEvent evento) {
        String gerado = UUID.randomUUID().toString();
        log.warn("contrato.outbox.sem_correlation_id contratoId={} eventId={} gerado={}",
                evento.contratoId(), evento.eventId(), gerado);
        return gerado;
    }
}
