package br.com.freela.contrato.infrastructure.messaging.outbox;

import java.time.Instant;
import java.util.UUID;

//estrutura especificada em docs/eventos.md
public record EventoEnvelope(
        UUID eventId,
        String eventType,
        int eventVersion,
        UUID contratoId,
        Instant occurredAt,
        String correlationId,
        String producer,
        Object payload
) {
}
