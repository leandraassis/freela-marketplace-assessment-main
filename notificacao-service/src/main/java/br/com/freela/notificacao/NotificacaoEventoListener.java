package br.com.freela.notificacao;

import br.com.freela.notificacao.evento.EventoContrato;
import br.com.freela.notificacao.evento.ProcessadorEventos;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import java.util.UUID;

@Component
class NotificacaoEventoListener {

    private static final Logger log = LoggerFactory.getLogger(NotificacaoEventoListener.class);

    private final ProcessadorEventos processador;
    private final NotificacaoService notificacaoService;
    private final String simulacaoFalharContratoId;

    NotificacaoEventoListener(
            ProcessadorEventos processador,
            NotificacaoService notificacaoService,
            @Value("${app.simulacao.falhar-contrato-id:}") String simulacaoFalharContratoId
    ) {
        this.processador = processador;
        this.notificacaoService = notificacaoService;
        this.simulacaoFalharContratoId = simulacaoFalharContratoId;
    }

    @KafkaListener(topics = "${app.kafka.topics.contratos}")
    void consumir(ConsumerRecord<String, String> record) {
        processador.processar(record, this::notificar, this::registrarDuplicado);
    }

    private void registrarDuplicado(EventoContrato evento) {
        log.info("notificacao.registro.duplicado eventId={} eventType={} contratoId={}",
                evento.eventId(), evento.eventType(), evento.contratoId());
    }

    private void notificar(EventoContrato evento) {
        log.info("notificacao.evento.recebido eventId={} eventType={} contratoId={}",
                evento.eventId(), evento.eventType(), evento.contratoId());

        if (evento.contratoId().toString().equals(simulacaoFalharContratoId)) {
            throw new IllegalStateException("falha simulada para o contrato " + evento.contratoId());
        }

        switch (evento.eventType()) {
            case "ContratoCriado" -> registrar(evento, "freelancerId", "CONTRATO_CRIADO",
                    "Voce foi contratado no contrato " + evento.contratoId());
            case "EntregaRegistrada" -> registrar(evento, "clienteId", "ENTREGA_REGISTRADA",
                    "A entrega do contrato " + evento.contratoId() + " foi registrada");
            case "ContratoConcluido" -> registrar(evento, "freelancerId", "CONTRATO_CONCLUIDO",
                    "O contrato " + evento.contratoId() + " foi concluido");
            case "ContratoCancelado" -> {
                String mensagem = "O contrato " + evento.contratoId() + " foi cancelado";
                registrar(evento, "clienteId", "CONTRATO_CANCELADO", mensagem);
                registrar(evento, "freelancerId", "CONTRATO_CANCELADO", mensagem);
            }
            default -> log.info("evento.ignorado eventId={} eventType={} contratoId={}",
                    evento.eventId(), evento.eventType(), evento.contratoId());
        }
    }

    private void registrar(EventoContrato evento, String campoDestinatario, String tipo, String mensagem) {
        notificacaoService.registrar(
                evento.eventId(),
                evento.contratoId(),
                uuid(evento.payload(), campoDestinatario),
                tipo,
                mensagem
        );
    }

    private UUID uuid(JsonNode payload, String campo) {
        JsonNode valor = payload.get(campo);
        if (valor == null || valor.isNull()) {
            throw new IllegalArgumentException("payload sem campo obrigatorio: " + campo);
        }
        return UUID.fromString(valor.asString());
    }
}
