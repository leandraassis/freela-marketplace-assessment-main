package br.com.freela.notificacao;
import org.slf4j.*; import org.springframework.web.bind.annotation.*; import java.util.*;

@RestController @RequestMapping("/api/notificacoes")
public class NotificacaoController {
    private static final Logger log=LoggerFactory.getLogger(NotificacaoController.class);
    private final NotificacaoService service;

    NotificacaoController(NotificacaoService service) {
        this.service = service;
    }

    @GetMapping
    public List<NotificacaoResponse> listar(
            @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId,
            @RequestParam(required = false) UUID contratoId,
            @RequestParam(required = false) UUID destinatarioId
    ) {
        log.info("http.notificacao.listar correlationId={} filtroContratoId={} filtroDestinatarioId={}",
                correlationId, contratoId, destinatarioId);
        return service.buscar(contratoId, destinatarioId)
                .stream()
                .map(NotificacaoResponse::de)
                .toList();
    }
}
