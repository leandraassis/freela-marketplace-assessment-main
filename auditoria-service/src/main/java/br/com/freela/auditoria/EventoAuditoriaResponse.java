package br.com.freela.auditoria;

import com.fasterxml.jackson.annotation.JsonRawValue;
import java.time.Instant;
import java.util.UUID;

record EventoAuditoriaResponse(
        UUID id,
        UUID eventId,
        String eventType,
        UUID contratoId,
        Instant occurredAt,
        String correlationId,
        Instant recebidoEm,
        @JsonRawValue String mensagem
) {

    static EventoAuditoriaResponse de(EventoAuditoria e) {
        return new EventoAuditoriaResponse(
                e.getId(),
                e.getEventId(),
                e.getEventType(),
                e.getAggregateId(),
                e.getOccurredAt(),
                e.getCorrelationId(),
                e.getRecebidoEm(),
                e.getPayload()
        );
    }
}
