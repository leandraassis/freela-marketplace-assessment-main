package br.com.freela.reputacao;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

record HistoricoReputacaoResponse(
        UUID id,
        UUID eventId,
        UUID freelancerId,
        UUID contratoId,
        String tipo,
        BigDecimal valor,
        Instant occurredAt
) {

    static HistoricoReputacaoResponse de(HistoricoReputacao h) {
        return new HistoricoReputacaoResponse(
                h.getId(),
                h.getEventId(),
                h.getFreelancerId(),
                h.getContratoId(),
                h.getTipo(),
                h.getValor(),
                h.getOccurredAt()
        );
    }
}
