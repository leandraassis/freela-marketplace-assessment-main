package br.com.freela.notificacao;

import java.time.Instant;
import java.util.UUID;

record NotificacaoResponse(
        UUID id,
        UUID eventId,
        UUID contratoId,
        UUID destinatarioId,
        String tipo,
        String mensagem,
        Instant criadaEm
) {

    static NotificacaoResponse de(Notificacao n) {
        return new NotificacaoResponse(
                n.getId(),
                n.getEventId(),
                n.getContratoId(),
                n.getDestinatarioId(),
                n.getTipo(),
                n.getMensagem(),
                n.getCriadaEm()
        );
    }
}
