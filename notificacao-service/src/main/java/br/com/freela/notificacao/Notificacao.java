package br.com.freela.notificacao;
import jakarta.persistence.*; import java.time.Instant; import java.util.UUID;
@Entity
@Table(
        name = "notificacoes",
        uniqueConstraints = @UniqueConstraint(
                name = "uk_notificacoes_event_id_destinatario_id",
                columnNames = {"event_id", "destinatario_id"}
        ),
        indexes = {
                @Index(name = "idx_notificacoes_contrato_id", columnList = "contrato_id"),
                @Index(name = "idx_notificacoes_destinatario_id", columnList = "destinatario_id")
        }
)
class Notificacao {
 @Id UUID id; UUID contratoId; UUID destinatarioId; String tipo; @Column(length=500) String mensagem; Instant criadaEm;

    @Column(nullable = false)
    private UUID eventId;

    protected Notificacao(){}

    Notificacao(
            UUID eventId,
            UUID contratoId,
            UUID destinatarioId,
            String tipo,
            String mensagem
    ) {
        this.id = UUID.randomUUID();
        this.eventId = eventId;
        this.contratoId = contratoId;
        this.destinatarioId = destinatarioId;
        this.tipo = tipo;
        this.mensagem = mensagem;
        this.criadaEm = Instant.now();
    }

    UUID getId() {
        return id;
    }

    UUID getEventId() {
        return eventId;
    }

    UUID getContratoId() {
        return contratoId;
    }

    UUID getDestinatarioId() {
        return destinatarioId;
    }

    String getTipo() {
        return tipo;
    }

    String getMensagem() {
        return mensagem;
    }

    Instant getCriadaEm() {
        return criadaEm;
    }
}
