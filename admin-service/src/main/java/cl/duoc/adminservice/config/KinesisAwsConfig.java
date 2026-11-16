package cl.duoc.adminservice.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;

/** Clientes de AWS que usa StreamAdminService. Las credenciales salen de las variables de entorno. */
@Configuration
public class KinesisAwsConfig {

    @Bean
    public KinesisClient kinesisClient(StreamAdminProperties props) {
        return KinesisClient.builder()
                .region(Region.of(props.region()))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
    }

    @Bean
    public DynamoDbClient dynamoDbClient(StreamAdminProperties props) {
        return DynamoDbClient.builder()
                .region(Region.of(props.region()))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
    }

    @Bean
    public CloudWatchClient cloudWatchClient(StreamAdminProperties props) {
        return CloudWatchClient.builder()
                .region(Region.of(props.region()))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
    }
}
