package br.com.freela.contrato.infrastructure.messaging.outbox;

import br.com.freela.contrato.domain.event.ContratoCancelado;
import br.com.freela.contrato.domain.event.ContratoConcluido;
import br.com.freela.contrato.domain.event.ContratoCriado;
import br.com.freela.contrato.domain.event.EntregaRegistrada;
import br.com.freela.contrato.domain.model.StatusContrato;
import br.com.freela.contrato.domain.shared.DomainEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Component
public class EventoEnvelopeMapper {

    static final int EVENT_VERSION = 1;

    private final JsonMapper jsonMapper;
    private final String producer;

    public EventoEnvelopeMapper(JsonMapper jsonMapper, @Value("${spring.application.name}") String producer) {
        this.jsonMapper = jsonMapper;
        this.producer = producer;
    }

    public String toJson(DomainEvent evento, String correlationId) {
        var envelope = new EventoEnvelope(
                evento.eventId(),
                evento.eventType(),
                EVENT_VERSION,
                evento.contratoId(),
                evento.occurredAt(),
                correlationId,
                producer,
                payload(evento)
        );
        return jsonMapper.writeValueAsString(envelope);
    }

    private Object payload(DomainEvent evento) {
        return switch (evento) {
            case ContratoCriado e -> new ContratoCriadoPayload(
                    e.contratoId(),
                    e.clienteId(),
                    e.freelancerId(),
                    e.titulo(),
                    e.valor(),
                    StatusContrato.ATIVO,
                    e.criadoEm()
            );
            case EntregaRegistrada e -> new EntregaRegistradaPayload(
                    e.contratoId(),
                    e.clienteId(),
                    e.freelancerId(),
                    StatusContrato.ENTREGA_REGISTRADA
            );
            case ContratoConcluido e -> new ContratoConcluidoPayload(
                    e.contratoId(),
                    e.clienteId(),
                    e.freelancerId(),
                    e.valor(),
                    StatusContrato.CONCLUIDO
            );
            case ContratoCancelado e -> new ContratoCanceladoPayload(
                    e.contratoId(),
                    e.clienteId(),
                    e.freelancerId(),
                    e.statusAnterior(),
                    StatusContrato.CANCELADO
            );
            default -> throw new IllegalArgumentException("Evento sem mapeamento para mensagem: " + evento.eventType());
        };
    }

    record ContratoCriadoPayload(
            UUID contratoId,
            UUID clienteId,
            UUID freelancerId,
            String titulo,
            BigDecimal valor,
            StatusContrato status,
            Instant criadoEm
    ) {
    }

    record EntregaRegistradaPayload(
            UUID contratoId,
            UUID clienteId,
            UUID freelancerId,
            StatusContrato status
    ) {
    }

    record ContratoConcluidoPayload(
            UUID contratoId,
            UUID clienteId,
            UUID freelancerId,
            BigDecimal valor,
            StatusContrato status
    ) {
    }

    record ContratoCanceladoPayload(
            UUID contratoId,
            UUID clienteId,
            UUID freelancerId,
            StatusContrato statusAnterior,
            StatusContrato status
    ) {
    }
}
