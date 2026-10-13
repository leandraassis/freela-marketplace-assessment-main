package br.com.freela.reputacao;

import java.math.BigDecimal;
import java.util.UUID;

record ReputacaoResponse(
        UUID freelancerId,
        int contratosConcluidos,
        int contratosCancelados,
        BigDecimal valorTotal
) {

    static ReputacaoResponse de(ReputacaoFreelancer r) {
        return new ReputacaoResponse(
                r.getFreelancerId(),
                r.getContratosConcluidos(),
                r.getContratosCancelados(),
                r.getValorTotal()
        );
    }
}
