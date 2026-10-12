package br.com.freela.contrato.infrastructure.web;

import br.com.freela.contrato.application.ContratoNaoEncontradoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(ContratoNaoEncontradoException.class)
    ProblemDetail naoEncontrado(ContratoNaoEncontradoException e) {
        return problema(HttpStatus.NOT_FOUND, "Contrato não encontrado", e.getMessage());
    }

    @ExceptionHandler(IllegalStateException.class)
    ProblemDetail transicaoInvalida(IllegalStateException e) {
        return problema(HttpStatus.CONFLICT, "Transição de status inválida", e.getMessage());
    }

    @ExceptionHandler(ObjectOptimisticLockingFailureException.class)
    ProblemDetail conflitoConcorrencia(ObjectOptimisticLockingFailureException e) {
        return problema(HttpStatus.CONFLICT, "Conflito de concorrência",
                "O contrato foi alterado por outra operação. Consulte o estado atual e tente novamente.");
    }

    @ExceptionHandler(IllegalArgumentException.class)
    ProblemDetail requisicaoInvalida(IllegalArgumentException e) {
        return problema(HttpStatus.BAD_REQUEST, "Requisição inválida", e.getMessage());
    }

    private ProblemDetail problema(HttpStatus status, String titulo, String detalhe) {
        log.warn("http.erro status={} titulo=\"{}\" detalhe=\"{}\"", status.value(), titulo, detalhe);
        var problem = ProblemDetail.forStatusAndDetail(status, detalhe);
        problem.setTitle(titulo);
        return problem;
    }
}
