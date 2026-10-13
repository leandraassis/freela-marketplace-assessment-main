package br.com.freela.contrato.infrastructure.messaging.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(
        name = "outbox_eventos",
        indexes = @Index(name = "idx_outbox_eventos_status_id", columnList = "status, id")
)
public class OutboxEventoJpaEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private UUID eventId;

    @Column(nullable = false, length = 80)
    private String eventType;

    @Column(nullable = false)
    private UUID contratoId;

    @Column(nullable = false, length = 120)
    private String correlationId;

    @Column(length = 255)
    private String traceContext;

    @Column(nullable = false, columnDefinition = "text")
    private String payload;

    @Column(nullable = false)
    private Instant occurredAt;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private OutboxStatus status;

    @Column(nullable = false)
    private int tentativas;

    @Column(length = 1000)
    private String ultimoErro;

    private Instant publicadoEm;

    protected OutboxEventoJpaEntity() {
    }

    public static OutboxEventoJpaEntity pendente(
            UUID eventId,
            String eventType,
            UUID contratoId,
            String correlationId,
            String payload,
            Instant occurredAt
    ) {
        var entity = new OutboxEventoJpaEntity();
        entity.eventId = eventId;
        entity.eventType = eventType;
        entity.contratoId = contratoId;
        entity.correlationId = correlationId;
        entity.payload = payload;
        entity.occurredAt = occurredAt;
        entity.status = OutboxStatus.PENDENTE;
        entity.tentativas = 0;
        return entity;
    }

    public void marcarPublicado(Instant publicadoEm) {
        this.status = OutboxStatus.PUBLICADO;
        this.publicadoEm = publicadoEm;
        this.ultimoErro = null;
    }

    public void registrarFalha(String erro) {
        this.tentativas++;
        this.ultimoErro = erro == null || erro.length() <= 1000 ? erro : erro.substring(0, 1000);
    }

    public Long getId() {
        return id;
    }

    public UUID getEventId() {
        return eventId;
    }

    public String getEventType() {
        return eventType;
    }

    public UUID getContratoId() {
        return contratoId;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getTraceContext() {
        return traceContext;
    }

    public String getPayload() {
        return payload;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public OutboxStatus getStatus() {
        return status;
    }

    public int getTentativas() {
        return tentativas;
    }

    public String getUltimoErro() {
        return ultimoErro;
    }

    public Instant getPublicadoEm() {
        return publicadoEm;
    }
}
