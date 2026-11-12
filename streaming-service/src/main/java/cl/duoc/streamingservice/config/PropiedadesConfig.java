package cl.duoc.streamingservice.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/** Registra StreamingProperties siempre (con el streaming apagado tambien se validan). */
@Configuration
@EnableConfigurationProperties(StreamingProperties.class)
public class PropiedadesConfig {
}
