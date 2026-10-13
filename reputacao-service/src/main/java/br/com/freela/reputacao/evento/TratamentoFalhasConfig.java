package br.com.freela.reputacao.evento;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import tools.jackson.core.JacksonException;
import java.nio.charset.StandardCharsets;

@Configuration
public class TratamentoFalhasConfig {

    private static final Logger log = LoggerFactory.getLogger(TratamentoFalhasConfig.class);

    @Bean
    public DefaultErrorHandler errorHandler(
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${app.kafka.topics.contratos-dlt}") String topicoDlt,
            @Value("${app.kafka.retry.intervalo-inicial-ms}") long intervaloInicialMs,
            @Value("${app.kafka.retry.multiplicador}") double multiplicador,
            @Value("${app.kafka.retry.max-retentativas}") int maxRetentativas
    ) {
        var publicadorDlt = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, erro) -> new TopicPartition(topicoDlt, record.partition())
        );

        var backOff = new ExponentialBackOffWithMaxRetries(maxRetentativas);
        backOff.setInitialInterval(intervaloInicialMs);
        backOff.setMultiplier(multiplicador);

        var errorHandler = new DefaultErrorHandler(
                (record, erro) -> enviarParaDlt(publicadorDlt, record, erro, topicoDlt),
                backOff
        );
        errorHandler.addNotRetryableExceptions(JacksonException.class, IllegalArgumentException.class);
        errorHandler.setCommitRecovered(true);
        return errorHandler;
    }

    @Bean
    public ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>> cabecalhoTentativa() {
        return container -> container.getContainerProperties().setDeliveryAttemptHeader(true);
    }

    private void enviarParaDlt(
            DeadLetterPublishingRecoverer publicadorDlt,
            ConsumerRecord<?, ?> record,
            Exception erro,
            String topicoDlt
    ) {
        publicadorDlt.accept(record, erro);

        Throwable causa = erro.getCause() != null ? erro.getCause() : erro;
        colocarMdc("correlationId", header(record, "correlationId"));
        colocarMdc("contratoId", record.key() == null ? null : record.key().toString());
        colocarMdc("eventId", header(record, "eventId"));
        try {
            log.error("evento.enviado.dlt topic={} partition={} offset={} topicoDlt={} eventType={} causa={}",
                    record.topic(), record.partition(), record.offset(), topicoDlt,
                    header(record, "eventType"),
                    causa.getClass().getSimpleName() + ": " + causa.getMessage());
        } finally {
            MDC.remove("correlationId");
            MDC.remove("contratoId");
            MDC.remove("eventId");
        }
    }

    private void colocarMdc(String chave, String valor) {
        if (valor != null) {
            MDC.put(chave, valor);
        }
    }

    private String header(ConsumerRecord<?, ?> record, String nome) {
        Header header = record.headers().lastHeader(nome);
        if (header == null || header.value() == null) {
            return null;
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
