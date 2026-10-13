package br.com.freela.reputacao;
import org.slf4j.*; import org.springframework.stereotype.Service; import org.springframework.transaction.annotation.Transactional; import java.math.BigDecimal; import java.util.UUID;
import br.com.freela.reputacao.evento.EventoContrato;
import java.util.List;
import java.util.Optional;

@Service
public class ReputacaoService {
    private static final Logger log=LoggerFactory.getLogger(ReputacaoService.class); private final ReputacaoRepository repository;
    private final HistoricoReputacaoRepository historicoRepository;

    public ReputacaoService(ReputacaoRepository repository, HistoricoReputacaoRepository historicoRepository) {
        this.repository = repository;
        this.historicoRepository = historicoRepository;
    }

    @Transactional
    public void registrarCriacao(EventoContrato evento, UUID freelancerId, BigDecimal valor) {
        registrarHistorico(evento, freelancerId, "CONTRATO_CRIADO", valor);
        var r = buscarOuCriar(freelancerId);
        repository.save(r);
        logSucesso(evento, r);
    }

    @Transactional
    public void registrarConclusao(EventoContrato evento, UUID freelancerId, BigDecimal valor) {
        registrarHistorico(evento, freelancerId, "CONTRATO_CONCLUIDO", valor);
        var r = buscarOuCriar(freelancerId);
        r.registrarConclusao(valor);
        repository.save(r);
        logSucesso(evento, r);
    }

    @Transactional
    public void registrarCancelamento(EventoContrato evento, UUID freelancerId) {
        registrarHistorico(evento, freelancerId, "CONTRATO_CANCELADO", null);
        var r = buscarOuCriar(freelancerId);
        r.registrarCancelamento();
        repository.save(r);
        logSucesso(evento, r);
    }

    @Transactional(readOnly = true)
    public Optional<ReputacaoFreelancer> buscar(UUID freelancerId) {
        return repository.findById(freelancerId);
    }

    @Transactional(readOnly = true)
    public List<ReputacaoFreelancer> listar() {
        return repository.findAll();
    }

    @Transactional(readOnly = true)
    public List<HistoricoReputacao> historico(UUID freelancerId) {
        return historicoRepository.findByFreelancerIdOrderByOccurredAt(freelancerId);
    }

    private void registrarHistorico(EventoContrato evento, UUID freelancerId, String tipo, BigDecimal valor) {
        log.info("reputacao.atualizacao.inicio contratoId={} freelancerId={} tipo={}", evento.contratoId(), freelancerId, tipo);
        historicoRepository.save(new HistoricoReputacao(
                evento.eventId(),
                freelancerId,
                evento.contratoId(),
                tipo,
                valor,
                evento.occurredAt()
        ));
    }

    private ReputacaoFreelancer buscarOuCriar(UUID freelancerId) {
        repository.criarSeNaoExiste(freelancerId);
        return repository.findById(freelancerId).orElseThrow();
    }

    private void logSucesso(EventoContrato evento, ReputacaoFreelancer r) {
        log.info("reputacao.atualizacao.sucesso contratoId={} freelancerId={} contratosConcluidos={} contratosCancelados={}",
                evento.contratoId(), r.getFreelancerId(), r.getContratosConcluidos(), r.getContratosCancelados());
    }
}
