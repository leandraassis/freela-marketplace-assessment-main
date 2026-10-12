package br.com.freela.contrato.infrastructure.messaging.outbox;

import org.springframework.data.jpa.repository.JpaRepository;

public interface SpringDataOutboxEventoRepository extends JpaRepository<OutboxEventoJpaEntity, Long> {
}
