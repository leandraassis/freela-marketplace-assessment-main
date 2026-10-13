package br.com.freela.reputacao;
import org.slf4j.*; import org.springframework.web.bind.annotation.*; import java.util.*;
import org.springframework.http.ResponseEntity;

@RestController @RequestMapping("/api/reputacoes")
public class ReputacaoController {
    private static final Logger log=LoggerFactory.getLogger(ReputacaoController.class);
    private final ReputacaoService service;

    ReputacaoController(ReputacaoService service) {
        this.service = service;
    }

    @GetMapping
    public List<ReputacaoResponse> listar(@RequestHeader(value = "X-Correlation-Id", required = false) String correlationId) {
        log.info("http.reputacao.listar correlationId={}", correlationId);
        return service.listar()
                .stream()
                .map(ReputacaoResponse::de)
                .toList();
    }

    @GetMapping("/{freelancerId}")
    public ResponseEntity<ReputacaoResponse> buscar(
            @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId,
            @PathVariable UUID freelancerId
    ) {
        log.info("http.reputacao.buscar correlationId={} freelancerId={}", correlationId, freelancerId);
        return service.buscar(freelancerId)
                .map(ReputacaoResponse::de)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/{freelancerId}/historico")
    public List<HistoricoReputacaoResponse> historico(
            @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId,
            @PathVariable UUID freelancerId
    ) {
        log.info("http.reputacao.historico correlationId={} freelancerId={}", correlationId, freelancerId);
        return service.historico(freelancerId)
                .stream()
                .map(HistoricoReputacaoResponse::de)
                .toList();
    }
}
