package br.com.freela.reputacao;
import jakarta.persistence.*; import java.math.BigDecimal; import java.util.UUID;
import org.hibernate.annotations.ColumnDefault;

@Entity @Table(name="reputacoes")
class ReputacaoFreelancer {
    @Id UUID freelancerId; int contratosConcluidos; BigDecimal valorTotal=BigDecimal.ZERO;

    @ColumnDefault("0")
    @Column(nullable = false)
    private int contratosCancelados;

    @Version
    @ColumnDefault("0")
    @Column(nullable = false)
    private Long versao;

    protected ReputacaoFreelancer(){} ReputacaoFreelancer(UUID id){this.freelancerId=id;}

    void registrarConclusao(BigDecimal valor) {
        contratosConcluidos++;
        valorTotal = valorTotal.add(valor);
    }

    void registrarCancelamento() {
        contratosCancelados++;
    }

    UUID getFreelancerId() {
        return freelancerId;
    }

    int getContratosConcluidos() {
        return contratosConcluidos;
    }

    int getContratosCancelados() {
        return contratosCancelados;
    }

    BigDecimal getValorTotal() {
        return valorTotal;
    }

    Long getVersao() {
        return versao;
    }
}
