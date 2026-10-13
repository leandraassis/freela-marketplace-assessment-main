package br.com.freela.reputacao;
import org.springframework.data.jpa.repository.JpaRepository; import java.util.UUID;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

interface ReputacaoRepository extends JpaRepository<ReputacaoFreelancer, UUID>{

    @Modifying
    @Query(value = """
            insert into reputacoes (freelancer_id, contratos_concluidos, contratos_cancelados, valor_total, versao)
            values (:freelancerId, 0, 0, 0, 0)
            on conflict (freelancer_id) do nothing
            """, nativeQuery = true)
    int criarSeNaoExiste(@Param("freelancerId") UUID freelancerId);
}
