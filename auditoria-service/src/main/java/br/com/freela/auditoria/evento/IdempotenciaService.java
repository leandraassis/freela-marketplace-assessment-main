package br.com.freela.auditoria.evento;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;

@Service
public class IdempotenciaService {

    private static final Logger log = LoggerFactory.getLogger(IdempotenciaService.class);

    private final EventoProcessadoRepository repository;

    public IdempotenciaService(EventoProcessadoRepository repository) {
        this.repository = repository;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public boolean registrarSeNovo(EventoContrato evento) {
        int inseridos = repository.inserirSeNovo(
                evento.eventId(),
                evento.eventType(),
                evento.contratoId(),
                Instant.now()
        );

        if (inseridos == 0) {
            log.info("evento.duplicado.ignorado eventId={} contratoId={} eventType={}",
                    evento.eventId(), evento.contratoId(), evento.eventType());
            return false;
        }
        return true;
    }
}
