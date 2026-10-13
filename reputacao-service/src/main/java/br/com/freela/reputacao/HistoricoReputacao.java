package br.com.freela.reputacao;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(
        name = "reputacao_historico",
        uniqueConstraints = @UniqueConstraint(name = "uk_reputacao_historico_event_id", columnNames = "event_id"),
        indexes = @Index(name = "idx_reputacao_historico_freelancer_id", columnList = "freelancer_id, occurred_at")
)
class HistoricoReputacao {

    @Id
    private UUID id;

    @Column(nullable = false)
    private UUID eventId;

    @Column(nullable = false)
    private UUID freelancerId;

    @Column(nullable = false)
    private UUID contratoId;

    @Column(nullable = false, length = 40)
    private String tipo;

    @Column(precision = 19, scale = 2)
    private BigDecimal valor;

    @Column(nullable = false)
    private Instant occurredAt;

    protected HistoricoReputacao() {
    }

    HistoricoReputacao(
            UUID eventId,
            UUID freelancerId,
            UUID contratoId,
            String tipo,
            BigDecimal valor,
            Instant occurredAt
    ) {
        this.id = UUID.randomUUID();
        this.eventId = eventId;
        this.freelancerId = freelancerId;
        this.contratoId = contratoId;
        this.tipo = tipo;
        this.valor = valor;
        this.occurredAt = occurredAt;
    }

    UUID getId() {
        return id;
    }

    UUID getEventId() {
        return eventId;
    }

    UUID getFreelancerId() {
        return freelancerId;
    }

    UUID getContratoId() {
        return contratoId;
    }

    String getTipo() {
        return tipo;
    }

    BigDecimal getValor() {
        return valor;
    }

    Instant getOccurredAt() {
        return occurredAt;
    }
}
