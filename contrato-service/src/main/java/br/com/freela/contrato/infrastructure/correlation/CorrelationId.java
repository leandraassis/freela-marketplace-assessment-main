package br.com.freela.contrato.infrastructure.correlation;

import org.slf4j.MDC;
import java.util.Optional;

public final class CorrelationId {

    public static final String HEADER = "X-Correlation-Id";
    public static final String MDC_KEY = "correlationId";
    public static final int TAMANHO_MAXIMO = 120;

    private CorrelationId() {
    }

    public static Optional<String> atual() {
        return Optional.ofNullable(MDC.get(MDC_KEY)).filter(id -> !id.isBlank());
    }

    public static boolean valido(String id) {
        return id != null && !id.isBlank() && id.length() <= TAMANHO_MAXIMO;
    }
}
