package br.com.freela.gateway;

import io.micrometer.tracing.exporter.SpanExportingPredicate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TracingConfig {

    @Bean
    public SpanExportingPredicate ignorarChamadasAoEureka() {
        return span -> !span.getTags().getOrDefault("http.url", "").contains("/eureka/");
    }
}
