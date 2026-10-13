package br.com.freela.auditoria;

import br.com.freela.auditoria.evento.ProcessadorEventos;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
class AuditoriaEventoListener {

    private final ProcessadorEventos processador;
    private final AuditoriaService auditoriaService;

    AuditoriaEventoListener(ProcessadorEventos processador, AuditoriaService auditoriaService) {
        this.processador = processador;
        this.auditoriaService = auditoriaService;
    }

    @KafkaListener(topics = "${app.kafka.topics.contratos}")
    void consumir(ConsumerRecord<String, String> record) {
        processador.processar(record, evento -> auditoriaService.registrar(evento, record.value()));
    }
}
