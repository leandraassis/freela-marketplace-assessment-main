package br.com.freela.auditoria;
import org.slf4j.*; import org.springframework.web.bind.annotation.*; import java.util.*;
import org.springframework.http.ResponseEntity;

@RestController @RequestMapping("/api/auditoria")
public class AuditoriaController {
 private static final Logger log=LoggerFactory.getLogger(AuditoriaController.class);
    private final AuditoriaService service;

    AuditoriaController(AuditoriaService service) {
        this.service = service;
    }

    @GetMapping
    public List<EventoAuditoriaResponse> listar(
            @RequestHeader(value = "X-Correlation-Id", required = false) String correlationIdRequisicao,
            @RequestParam(required = false) UUID contratoId,
            @RequestParam(required = false) String eventType,
            @RequestParam(required = false) String correlationId
    ) {
        log.info("http.auditoria.listar correlationIdRequisicao={} filtroContratoId={} filtroEventType={} filtroCorrelationId={}",
                correlationIdRequisicao, contratoId, eventType, correlationId);
        return service.buscar(contratoId, eventType, correlationId)
                .stream()
                .map(EventoAuditoriaResponse::de)
                .toList();
    }

    @GetMapping("/eventos/{eventId}")
    public ResponseEntity<EventoAuditoriaResponse> buscarPorEventId(
            @RequestHeader(value = "X-Correlation-Id", required = false) String correlationIdRequisicao,
            @PathVariable UUID eventId
    ) {
        log.info("http.auditoria.buscar correlationIdRequisicao={} eventId={}", correlationIdRequisicao, eventId);
        return service.buscarPorEventId(eventId)
                .map(EventoAuditoriaResponse::de)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
