package br.com.freela.contrato.domain.model;

import br.com.freela.contrato.domain.shared.DomainEvent;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ContratoTest {

    private static final UUID CLIENTE_ID = UUID.randomUUID();
    private static final UUID FREELANCER_ID = UUID.randomUUID();

    @Test
    void criarGeraContratoCriadoComStatusAtivo() {
        Contrato contrato = novoContrato();

        assertThat(contrato.status()).isEqualTo(StatusContrato.ATIVO);
        assertThat(tipos(contrato.pullDomainEvents())).containsExactly("ContratoCriado");
    }

    @Test
    void entregaEConclusaoGeramEventosNaOrdem() {
        Contrato contrato = novoContrato();

        contrato.registrarEntrega();
        contrato.concluir();

        assertThat(contrato.status()).isEqualTo(StatusContrato.CONCLUIDO);
        assertThat(tipos(contrato.pullDomainEvents()))
                .containsExactly("ContratoCriado", "EntregaRegistrada", "ContratoConcluido");
    }

    @Test
    void cancelarContratoAtivoGeraContratoCancelado() {
        Contrato contrato = novoContrato();
        contrato.pullDomainEvents();

        contrato.cancelar();

        assertThat(contrato.status()).isEqualTo(StatusContrato.CANCELADO);
        assertThat(tipos(contrato.pullDomainEvents())).containsExactly("ContratoCancelado");
    }

    @Test
    void eventosTrazemOContratoEIdentificadorUnico() {
        Contrato contrato = novoContrato();
        contrato.registrarEntrega();

        List<DomainEvent> eventos = contrato.pullDomainEvents();

        assertThat(eventos).allSatisfy(evento -> assertThat(evento.contratoId()).isEqualTo(contrato.id()));
        assertThat(eventos).extracting(DomainEvent::eventId).doesNotHaveDuplicates();
    }

    @Test
    void concluirSemEntregaLancaExcecaoESemEvento() {
        Contrato contrato = novoContrato();
        contrato.pullDomainEvents();

        assertThatThrownBy(contrato::concluir).isInstanceOf(IllegalStateException.class);
        assertThat(contrato.status()).isEqualTo(StatusContrato.ATIVO);
        assertThat(contrato.pullDomainEvents()).isEmpty();
    }

    @Test
    void entregaDuplicadaLancaExcecao() {
        Contrato contrato = novoContrato();
        contrato.registrarEntrega();

        assertThatThrownBy(contrato::registrarEntrega).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void cancelarAposEntregaLancaExcecao() {
        Contrato contrato = novoContrato();
        contrato.registrarEntrega();

        assertThatThrownBy(contrato::cancelar).isInstanceOf(IllegalStateException.class);
        assertThat(contrato.status()).isEqualTo(StatusContrato.ENTREGA_REGISTRADA);
    }

    @Test
    void criarComValorInvalidoLancaExcecao() {
        assertThatThrownBy(() -> Contrato.criar(CLIENTE_ID, FREELANCER_ID, "Titulo", BigDecimal.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private Contrato novoContrato() {
        return Contrato.criar(CLIENTE_ID, FREELANCER_ID, "Desenvolvimento de API", new BigDecimal("1500.00"));
    }

    private List<String> tipos(List<DomainEvent> eventos) {
        return eventos.stream()
                .map(DomainEvent::eventType)
                .toList();
    }
}
