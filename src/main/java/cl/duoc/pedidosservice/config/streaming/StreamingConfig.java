package cl.duoc.pedidosservice.config.streaming;

import java.time.Duration;
import java.util.Locale;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;

/** Cliente de Kinesis: region, credenciales (cadena por defecto), reintentos y timeouts. */
@Configuration
@EnableConfigurationProperties(StreamingProperties.class)
public class StreamingConfig {

    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(name = "streaming.habilitado", havingValue = "true")
    public KinesisClient kinesisClient(StreamingProperties props) {
        StreamingProperties.Cliente c = props.cliente();
        return KinesisClient.builder()
                .region(Region.of(props.region()))
                .credentialsProvider(DefaultCredentialsProvider.builder().build())
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .apiCallTimeout(Duration.ofSeconds(c.apiCallTimeoutSegundos()))
                        .apiCallAttemptTimeout(Duration.ofSeconds(c.apiCallAttemptTimeoutSegundos()))
                        .retryStrategy(RetryMode.valueOf(c.retryMode().toUpperCase(Locale.ROOT)))
                        .build())
                .build();
    }
}
