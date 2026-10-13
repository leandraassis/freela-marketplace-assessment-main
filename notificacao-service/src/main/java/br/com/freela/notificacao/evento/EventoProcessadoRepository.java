package br.com.freela.notificacao.evento;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import java.time.Instant;
import java.util.UUID;

interface EventoProcessadoRepository extends JpaRepository<EventoProcessado, UUID> {

    @Modifying
    @Query(value = """
            insert into eventos_processados (event_id, event_type, contrato_id, processado_em)
            values (:eventId, :eventType, :contratoId, :processadoEm)
            on conflict (event_id) do nothing
            """, nativeQuery = true)
    int inserirSeNovo(
            @Param("eventId") UUID eventId,
            @Param("eventType") String eventType,
            @Param("contratoId") UUID contratoId,
            @Param("processadoEm") Instant processadoEm
    );
}
