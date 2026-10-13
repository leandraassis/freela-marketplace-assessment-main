package br.com.freela.notificacao.evento;

import org.springframework.stereotype.Component;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectReader;
import tools.jackson.databind.json.JsonMapper;

@Component
public class EventoContratoParser {

    private final ObjectReader reader;

    public EventoContratoParser(JsonMapper jsonMapper) {
        this.reader = jsonMapper
                .readerFor(EventoContrato.class)
                .without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    public EventoContrato parse(String json) {
        if (json == null || json.isBlank()) {
            throw new IllegalArgumentException("mensagem vazia");
        }

        EventoContrato evento = reader.readValue(json);
        exigir(evento.eventId(), "eventId");
        exigir(evento.eventType(), "eventType");
        exigir(evento.eventVersion(), "eventVersion");
        exigir(evento.contratoId(), "contratoId");
        exigir(evento.payload(), "payload");
        return evento;
    }

    private void exigir(Object valor, String campo) {
        if (valor == null) {
            throw new IllegalArgumentException("envelope sem campo obrigatorio: " + campo);
        }
    }
}
