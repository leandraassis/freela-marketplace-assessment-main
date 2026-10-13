package br.com.freela.notificacao;
import org.springframework.data.jpa.repository.JpaRepository; import java.util.UUID;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import java.util.List;

interface NotificacaoRepository extends JpaRepository<Notificacao, UUID>{

    @Query("""
            select n from Notificacao n
            where (:contratoId is null or n.contratoId = :contratoId)
              and (:destinatarioId is null or n.destinatarioId = :destinatarioId)
            order by n.criadaEm
            """)
    List<Notificacao> buscar(
            @Param("contratoId") UUID contratoId,
            @Param("destinatarioId") UUID destinatarioId
    );
}
