package br.com.freela.reputacao;

import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ReputacaoConsumoIntegrationTest extends IntegracaoTestBase {

    @Test
    void contratoConcluidoAtualizaReputacaoEHistorico() {
        UUID freelancerId = UUID.randomUUID();
        UUID contratoId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();

        publicar(eventId, "ContratoConcluido", contratoId, payloadDe(contratoId, freelancerId));
        aguardarProcessado(eventId);

        Map<String, Object> reputacao = reputacao(freelancerId);
        assertThat(reputacao.get("contratos_concluidos")).isEqualTo(1);
        assertThat((BigDecimal) reputacao.get("valor_total")).isEqualByComparingTo("350.00");
        assertThat(jdbc.queryForObject(
                "select tipo from reputacao_historico where event_id = ?", String.class, eventId))
                .isEqualTo("CONTRATO_CONCLUIDO");
    }

    @Test
    void mensagemDuplicadaIncrementaReputacaoUmaUnicaVez() {
        UUID freelancerId = UUID.randomUUID();
        UUID contratoId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        UUID marcador = UUID.randomUUID();

        publicar(eventId, "ContratoConcluido", contratoId, payloadDe(contratoId, freelancerId));
        publicar(eventId, "ContratoConcluido", contratoId, payloadDe(contratoId, freelancerId));
        publicar(marcador, "EntregaRegistrada", contratoId, payloadDe(contratoId, freelancerId));
        aguardarProcessado(marcador);

        Map<String, Object> reputacao = reputacao(freelancerId);
        assertThat(reputacao.get("contratos_concluidos")).isEqualTo(1);
        assertThat((BigDecimal) reputacao.get("valor_total")).isEqualByComparingTo("350.00");
        assertThat(jdbc.queryForObject(
                "select count(*) from reputacao_historico where event_id = ?", Integer.class, eventId))
                .isEqualTo(1);
    }

    @Test
    void contratoCanceladoIncrementaCancelados() {
        UUID freelancerId = UUID.randomUUID();
        UUID contratoId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();

        publicar(eventId, "ContratoCancelado", contratoId, payloadDe(contratoId, freelancerId));
        aguardarProcessado(eventId);

        Map<String, Object> reputacao = reputacao(freelancerId);
        assertThat(reputacao.get("contratos_cancelados")).isEqualTo(1);
        assertThat(reputacao.get("contratos_concluidos")).isEqualTo(0);
    }

    private Map<String, Object> reputacao(UUID freelancerId) {
        return jdbc.queryForMap(
                "select contratos_concluidos, contratos_cancelados, valor_total from reputacoes where freelancer_id = ?",
                freelancerId);
    }

    private String payloadDe(UUID contratoId, UUID freelancerId) {
        return payload(contratoId, UUID.randomUUID(), freelancerId, new BigDecimal("350.00"));
    }
}
