package br.com.freela.contrato.application;

import br.com.freela.contrato.domain.shared.DomainEvent;

public interface EventoOutbox {

    void registrar(DomainEvent evento);
}
