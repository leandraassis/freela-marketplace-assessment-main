package br.com.freela.contrato.application;

import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.domain.repository.ContratoRepository;
import br.com.freela.contrato.domain.shared.DomainEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

@Service
public class ContratoApplicationService {
    private static final Logger log = LoggerFactory.getLogger(ContratoApplicationService.class);
    private final ContratoRepository repository;

    public ContratoApplicationService(ContratoRepository repository) { this.repository = repository; }

    @Transactional
    public Contrato criar(CriarContratoCommand cmd) {
        log.info("contrato.criacao.inicio clienteId={} freelancerId={} titulo={} valor={}",
                cmd.clienteId(), cmd.freelancerId(), cmd.titulo(), cmd.valor());
        Contrato contrato = Contrato.criar(cmd.clienteId(), cmd.freelancerId(), cmd.titulo(), cmd.valor());
        log.info("contrato.dominio.criado contratoId={} status={} domainEvents={}",
                contrato.id(), contrato.status(), contrato.domainEvents().size());
        Contrato salvo = repository.salvar(contrato);
        registrarEventos(contrato);
        log.info("contrato.criacao.sucesso contratoId={} clienteId={} freelancerId={} status={}",
                salvo.id(), salvo.clienteId(), salvo.freelancerId(), salvo.status());
        return salvo;
    }

    @Transactional
    public Contrato registrarEntrega(UUID id) {
        return executarTransicao(id, "entrega", Contrato::registrarEntrega);
    }

    @Transactional
    public Contrato concluir(UUID id) {
        return executarTransicao(id, "conclusao", Contrato::concluir);
    }

    @Transactional
    public Contrato cancelar(UUID id) {
        return executarTransicao(id, "cancelamento", Contrato::cancelar);
    }

    @Transactional(readOnly = true)
    public Contrato buscar(UUID id) {
        log.info("contrato.busca.inicio contratoId={}", id);
        var contrato = carregar(id);
        log.info("contrato.busca.sucesso contratoId={} status={}", id, contrato.status());
        return contrato;
    }

    @Transactional(readOnly = true)
    public List<Contrato> listar() {
        log.info("contrato.listagem.inicio");
        var contratos = repository.listar();
        log.info("contrato.listagem.sucesso quantidade={}", contratos.size());
        return contratos;
    }

    private Contrato executarTransicao(UUID id, String operacao, Consumer<Contrato> transicao) {
        log.info("contrato.{}.inicio contratoId={}", operacao, id);
        Contrato contrato = carregar(id);
        var statusAnterior = contrato.status();
        try {
            transicao.accept(contrato);
        } catch (IllegalStateException e) {
            log.warn("contrato.{}.rejeitada contratoId={} status={} motivo={}", operacao, id, statusAnterior, e.getMessage());
            throw e;
        }
        Contrato salvo = repository.salvar(contrato);
        registrarEventos(contrato);
        log.info("contrato.{}.sucesso contratoId={} statusAnterior={} status={}", operacao, id, statusAnterior, salvo.status());
        return salvo;
    }

    private Contrato carregar(UUID id) {
        return repository.buscarPorId(id).orElseThrow(() -> {
            log.warn("contrato.nao_encontrado contratoId={}", id);
            return new ContratoNaoEncontradoException(id);
        });
    }

    private void registrarEventos(Contrato contrato) {
        for (DomainEvent event : contrato.pullDomainEvents()) {
            log.info("contrato.evento.pendente contratoId={} eventId={} eventType={} occurredAt={}",
                    event.contratoId(), event.eventId(), event.eventType(), event.occurredAt());
        }
    }
}
