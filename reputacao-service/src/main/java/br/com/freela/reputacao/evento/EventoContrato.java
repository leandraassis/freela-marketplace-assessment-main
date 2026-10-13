package br.com.freela.reputacao.evento;

import tools.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

public record EventoContrato(
        UUID eventId,
        String eventType,
        Integer eventVersion,
        UUID contratoId,
        Instant occurredAt,
        String correlationId,
        String producer,
        JsonNode payload
) {
}
