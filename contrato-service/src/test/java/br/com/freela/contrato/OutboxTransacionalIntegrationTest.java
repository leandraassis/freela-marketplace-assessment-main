package br.com.freela.contrato;

import br.com.freela.contrato.application.ContratoApplicationService;
import br.com.freela.contrato.application.CriarContratoCommand;
import br.com.freela.contrato.domain.model.Contrato;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OutboxTransacionalIntegrationTest extends IntegracaoTestBase {

    @Autowired
    private ContratoApplicationService service;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    private JdbcTemplate jdbc;

    @Test
    void criarContratoGravaContratoEEventoNaOutbox() {
        Contrato contrato = service.criar(comando());

        assertThat(contarContratos(contrato.id())).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                "select event_type from outbox_eventos where contrato_id = ?", String.class, contrato.id()))
                .isEqualTo("ContratoCriado");
    }

    @Test
    void rollbackDaTransacaoNaoGravaContratoNemEvento() {
        AtomicReference<UUID> contratoId = new AtomicReference<>();

        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status -> {
            contratoId.set(service.criar(comando()).id());
            throw new IllegalStateException("falha depois de gravar");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(contratoId.get()).isNotNull();
        assertThat(contarContratos(contratoId.get())).isZero();
        assertThat(contarEventos(contratoId.get())).isZero();
    }

    @Test
    void transicaoInvalidaNaoGravaEvento() {
        Contrato contrato = service.criar(comando());

        assertThatThrownBy(() -> service.concluir(contrato.id())).isInstanceOf(IllegalStateException.class);

        assertThat(contarEventos(contrato.id())).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                "select status from contratos where id = ?", String.class, contrato.id()))
                .isEqualTo("ATIVO");
    }

    private CriarContratoCommand comando() {
        return new CriarContratoCommand(UUID.randomUUID(), UUID.randomUUID(), "Teste outbox", new BigDecimal("100.00"));
    }

    private int contarContratos(UUID contratoId) {
        return jdbc.queryForObject("select count(*) from contratos where id = ?", Integer.class, contratoId);
    }

    private int contarEventos(UUID contratoId) {
        return jdbc.queryForObject("select count(*) from outbox_eventos where contrato_id = ?", Integer.class, contratoId);
    }
}
