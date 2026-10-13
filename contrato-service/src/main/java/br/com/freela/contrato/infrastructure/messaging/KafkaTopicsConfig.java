package br.com.freela.contrato.infrastructure.messaging;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicsConfig {

    private final int particoes;

    public KafkaTopicsConfig(@Value("${app.kafka.topics.particoes}") int particoes) {
        this.particoes = particoes;
    }

    @Bean
    public NewTopic contratosTopic(@Value("${app.kafka.topics.contratos}") String nome) {
        return TopicBuilder.name(nome)
                .partitions(particoes)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic contratosDltTopic(@Value("${app.kafka.topics.contratos-dlt}") String nome) {
        return TopicBuilder.name(nome)
                .partitions(particoes)
                .replicas(1)
                .build();
    }
}
