package br.com.freela.auditoria;
import org.springframework.data.jpa.repository.JpaRepository; import java.util.UUID;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import java.util.List;
import java.util.Optional;

interface EventoAuditoriaRepository extends JpaRepository<EventoAuditoria, UUID>{

    Optional<EventoAuditoria> findByEventId(UUID eventId);

    @Query("""
            select e from EventoAuditoria e
            where (:contratoId is null or e.aggregateId = :contratoId)
              and (:eventType is null or e.eventType = :eventType)
              and (:correlationId is null or e.correlationId = :correlationId)
            order by e.occurredAt, e.recebidoEm
            """)
    List<EventoAuditoria> buscar(
            @Param("contratoId") UUID contratoId,
            @Param("eventType") String eventType,
            @Param("correlationId") String correlationId
    );
}
