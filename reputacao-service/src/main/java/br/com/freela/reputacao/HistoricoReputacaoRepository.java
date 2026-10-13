package br.com.freela.reputacao;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.UUID;

interface HistoricoReputacaoRepository extends JpaRepository<HistoricoReputacao, UUID> {

    List<HistoricoReputacao> findByFreelancerIdOrderByOccurredAt(UUID freelancerId);
}
