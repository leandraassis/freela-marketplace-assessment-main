package br.com.freela.auditoria;
import org.slf4j.*; import org.springframework.stereotype.Service; import org.springframework.transaction.annotation.Transactional; import java.util.UUID;
import br.com.freela.auditoria.evento.EventoContrato;
import java.util.List;
import java.util.Optional;

@Service
public class AuditoriaService {
 private static final Logger log=LoggerFactory.getLogger(AuditoriaService.class); private final EventoAuditoriaRepository repository;
 public AuditoriaService(EventoAuditoriaRepository r){this.repository=r;}

    @Transactional
    public void registrar(EventoContrato evento, String mensagem) {
        log.info("auditoria.registro.inicio eventId={} contratoId={} eventType={}",
                evento.eventId(), evento.contratoId(), evento.eventType());

        var e = repository.save(new EventoAuditoria(
                evento.eventId(),
                evento.contratoId(),
                evento.eventType(),
                evento.occurredAt(),
                evento.correlationId(),
                mensagem
        ));

        log.info("auditoria.registro.sucesso auditoriaId={} eventId={} contratoId={} eventType={}",
                e.getId(), evento.eventId(), evento.contratoId(), evento.eventType());
    }

    @Transactional(readOnly = true)
    public List<EventoAuditoria> buscar(UUID contratoId, String eventType, String correlationId) {
        return repository.buscar(contratoId, eventType, correlationId);
    }

    @Transactional(readOnly = true)
    public Optional<EventoAuditoria> buscarPorEventId(UUID eventId) {
        return repository.findByEventId(eventId);
    }
}
