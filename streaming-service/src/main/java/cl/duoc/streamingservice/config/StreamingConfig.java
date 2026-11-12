package cl.duoc.streamingservice.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.kinesis.common.KinesisClientUtil;

/**
 * Clientes de AWS: region de la configuracion y credenciales de la cadena por defecto.
 * La KCL necesita los tres clientes asincronos; los sincronos los usa nuestro codigo
 * (stream de fallidos, tablas de contadores y de eventos procesados).
 */
@Configuration
@ConditionalOnProperty(name = "streaming.habilitado", havingValue = "true")
public class StreamingConfig {

    @Bean(destroyMethod = "close")
    public KinesisAsyncClient kinesisAsyncClient(StreamingProperties props) {
        return KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder()
                .region(Region.of(props.region()))
                .credentialsProvider(DefaultCredentialsProvider.builder().build()));
    }

    @Bean(destroyMethod = "close")
    public DynamoDbAsyncClient dynamoDbAsyncClient(StreamingProperties props) {
        return DynamoDbAsyncClient.builder()
                .region(Region.of(props.region()))
                .credentialsProvider(DefaultCredentialsProvider.builder().build())
                .build();
    }

    @Bean(destroyMethod = "close")
    public CloudWatchAsyncClient cloudWatchAsyncClient(StreamingProperties props) {
        return CloudWatchAsyncClient.builder()
                .region(Region.of(props.region()))
                .credentialsProvider(DefaultCredentialsProvider.builder().build())
                .build();
    }

    @Bean(destroyMethod = "close")
    public KinesisClient kinesisClient(StreamingProperties props) {
        return KinesisClient.builder()
                .region(Region.of(props.region()))
                .credentialsProvider(DefaultCredentialsProvider.builder().build())
                .build();
    }

    @Bean(destroyMethod = "close")
    public DynamoDbClient dynamoDbClient(StreamingProperties props) {
        return DynamoDbClient.builder()
                .region(Region.of(props.region()))
                .credentialsProvider(DefaultCredentialsProvider.builder().build())
                .build();
    }
}
