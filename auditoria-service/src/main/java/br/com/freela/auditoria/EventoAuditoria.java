package br.com.freela.auditoria;
import jakarta.persistence.*; import java.time.Instant; import java.util.UUID;
@Entity
@Table(
        name = "auditoria_eventos",
        uniqueConstraints = @UniqueConstraint(name = "uk_auditoria_eventos_event_id", columnNames = "event_id"),
        indexes = {
                @Index(name = "idx_auditoria_eventos_aggregate_id", columnList = "aggregate_id"),
                @Index(name = "idx_auditoria_eventos_correlation_id", columnList = "correlation_id")
        }
)
class EventoAuditoria {
 @Id UUID id; UUID eventId; UUID aggregateId; String eventType; String correlationId; @Column(columnDefinition="text") String payload; Instant recebidoEm;

    @Column(nullable = false)
    private Instant occurredAt;

    protected EventoAuditoria(){}

    EventoAuditoria(
            UUID eventId,
            UUID aggregateId,
            String eventType,
            Instant occurredAt,
            String correlationId,
            String payload
    ) {
        this.id = UUID.randomUUID();
        this.eventId = eventId;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.occurredAt = occurredAt;
        this.correlationId = correlationId;
        this.payload = payload;
        this.recebidoEm = Instant.now();
    }

    UUID getId() {
        return id;
    }

    UUID getEventId() {
        return eventId;
    }

    UUID getAggregateId() {
        return aggregateId;
    }

    String getEventType() {
        return eventType;
    }

    Instant getOccurredAt() {
        return occurredAt;
    }

    String getCorrelationId() {
        return correlationId;
    }

    String getPayload() {
        return payload;
    }

    Instant getRecebidoEm() {
        return recebidoEm;
    }
}
