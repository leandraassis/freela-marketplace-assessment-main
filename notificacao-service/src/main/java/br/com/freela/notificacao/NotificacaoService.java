package br.com.freela.notificacao;
import org.slf4j.*; import org.springframework.stereotype.Service; import org.springframework.transaction.annotation.Transactional; import java.util.UUID;
import java.util.List;

@Service
public class NotificacaoService {
 private static final Logger log=LoggerFactory.getLogger(NotificacaoService.class); private final NotificacaoRepository repository;
 public NotificacaoService(NotificacaoRepository r){this.repository=r;}

    @Transactional
    public void registrar(UUID eventId, UUID contratoId, UUID destinatarioId, String tipo, String mensagem) {
        log.info("notificacao.registro.inicio contratoId={} destinatarioId={} tipo={}", contratoId, destinatarioId, tipo);
        try {
            var n = repository.saveAndFlush(new Notificacao(eventId, contratoId, destinatarioId, tipo, mensagem));
            log.info("notificacao.registro.sucesso notificacaoId={} contratoId={} destinatarioId={}",
                    n.getId(), contratoId, destinatarioId);
        } catch (RuntimeException e) {
            log.error("notificacao.registro.falha eventId={} contratoId={} destinatarioId={} causa={}",
                    eventId, contratoId, destinatarioId, e.getClass().getSimpleName());
            throw e;
        }
    }

    @Transactional(readOnly = true)
    public List<Notificacao> buscar(UUID contratoId, UUID destinatarioId) {
        return repository.buscar(contratoId, destinatarioId);
    }
}
