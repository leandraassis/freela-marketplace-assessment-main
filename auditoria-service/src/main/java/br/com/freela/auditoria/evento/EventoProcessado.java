package br.com.freela.auditoria.evento;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "eventos_processados")
public class EventoProcessado {

    @Id
    private UUID eventId;

    @Column(nullable = false, length = 80)
    private String eventType;

    @Column(nullable = false)
    private UUID contratoId;

    @Column(nullable = false)
    private Instant processadoEm;

    protected EventoProcessado() {
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

    public Instant getProcessadoEm() {
        return processadoEm;
    }
}
