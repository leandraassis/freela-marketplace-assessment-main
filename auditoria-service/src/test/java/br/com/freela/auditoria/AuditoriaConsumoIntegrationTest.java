package br.com.freela.auditoria;

import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class AuditoriaConsumoIntegrationTest extends IntegracaoTestBase {

    private static final List<String> TIPOS = List.of("ContratoCriado", "EntregaRegistrada", "ContratoConcluido");

    @Test
    void eventoPublicadoEhRegistradoNaAuditoria() {
        UUID eventId = UUID.randomUUID();
        UUID contratoId = UUID.randomUUID();

        publicar(eventId, "ContratoCriado", contratoId, payloadDe(contratoId));
        aguardarProcessado(eventId);

        Map<String, Object> registro = jdbc.queryForMap(
                "select event_type, aggregate_id, correlation_id, occurred_at, payload from auditoria_eventos where event_id = ?",
                eventId);
        assertThat(registro.get("event_type")).isEqualTo("ContratoCriado");
        assertThat(registro.get("aggregate_id")).isEqualTo(contratoId);
        assertThat(registro.get("correlation_id")).isEqualTo("teste-ContratoCriado");
        assertThat(registro.get("occurred_at")).isNotNull();
        assertThat((String) registro.get("payload")).contains(eventId.toString());
    }

    @Test
    void mensagemDuplicadaGeraUmUnicoRegistro() {
        UUID eventId = UUID.randomUUID();
        UUID marcador = UUID.randomUUID();
        UUID contratoId = UUID.randomUUID();

        publicar(eventId, "ContratoCriado", contratoId, payloadDe(contratoId));
        publicar(eventId, "ContratoCriado", contratoId, payloadDe(contratoId));
        publicar(marcador, "EntregaRegistrada", contratoId, payloadDe(contratoId));
        aguardarProcessado(marcador);

        assertThat(jdbc.queryForObject(
                "select count(*) from auditoria_eventos where event_id = ?", Integer.class, eventId))
                .isEqualTo(1);
    }

    @Test
    void eventosDoMesmoContratoSaoProcessadosNaOrdemDeEnvio() {
        UUID contratoId = UUID.randomUUID();
        List<UUID> enviados = new ArrayList<>();

        for (int i = 0; i < 9; i++) {
            UUID eventId = UUID.randomUUID();
            enviados.add(eventId);
            publicar(eventId, TIPOS.get(i % TIPOS.size()), contratoId, payloadDe(contratoId));
        }
        aguardarProcessado(enviados.getLast());

        List<UUID> processados = jdbc.queryForList(
                "select event_id from auditoria_eventos where aggregate_id = ? order by recebido_em", UUID.class, contratoId);
        assertThat(processados).containsExactlyElementsOf(enviados);
    }

    private String payloadDe(UUID contratoId) {
        return payload(contratoId, UUID.randomUUID(), UUID.randomUUID(), new BigDecimal("100.00"));
    }
}
