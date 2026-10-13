package br.com.freela.reputacao;

import br.com.freela.reputacao.evento.EventoContrato;
import br.com.freela.reputacao.evento.ProcessadorEventos;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.util.UUID;

@Component
class ReputacaoEventoListener {

    private static final Logger log = LoggerFactory.getLogger(ReputacaoEventoListener.class);

    private final ProcessadorEventos processador;
    private final ReputacaoService reputacaoService;

    ReputacaoEventoListener(ProcessadorEventos processador, ReputacaoService reputacaoService) {
        this.processador = processador;
        this.reputacaoService = reputacaoService;
    }

    @KafkaListener(topics = "${app.kafka.topics.contratos}")
    void consumir(ConsumerRecord<String, String> record) {
        processador.processar(record, this::atualizar);
    }

    private void atualizar(EventoContrato evento) {
        JsonNode payload = evento.payload();
        switch (evento.eventType()) {
            case "ContratoCriado" -> reputacaoService.registrarCriacao(
                    evento,
                    uuid(payload, "freelancerId"),
                    decimal(payload, "valor")
            );
            case "ContratoConcluido" -> reputacaoService.registrarConclusao(
                    evento,
                    uuid(payload, "freelancerId"),
                    decimal(payload, "valor")
            );
            case "ContratoCancelado" -> reputacaoService.registrarCancelamento(
                    evento,
                    uuid(payload, "freelancerId")
            );
            default -> log.info("evento.ignorado eventId={} eventType={} contratoId={}",
                    evento.eventId(), evento.eventType(), evento.contratoId());
        }
    }

    private UUID uuid(JsonNode payload, String campo) {
        return UUID.fromString(obrigatorio(payload, campo).asString());
    }

    private BigDecimal decimal(JsonNode payload, String campo) {
        JsonNode valor = obrigatorio(payload, campo);
        if (!valor.isNumber()) {
            throw new IllegalArgumentException("payload com campo nao numerico: " + campo);
        }
        return valor.decimalValue();
    }

    private JsonNode obrigatorio(JsonNode payload, String campo) {
        JsonNode valor = payload.get(campo);
        if (valor == null || valor.isNull()) {
            throw new IllegalArgumentException("payload sem campo obrigatorio: " + campo);
        }
        return valor;
    }
}
