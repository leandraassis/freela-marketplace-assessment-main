package br.com.freela.contrato.infrastructure.messaging.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import java.util.List;

public interface SpringDataOutboxEventoRepository extends JpaRepository<OutboxEventoJpaEntity, Long> {

    @Query(value = "select pg_try_advisory_xact_lock(:chave)", nativeQuery = true)
    boolean tentarLockPublicacao(@Param("chave") long chave);

    @Query(
            value = "select * from outbox_eventos where status = 'PENDENTE' order by id limit :lote",
            nativeQuery = true
    )
    List<OutboxEventoJpaEntity> buscarPendentes(@Param("lote") int lote);
}
