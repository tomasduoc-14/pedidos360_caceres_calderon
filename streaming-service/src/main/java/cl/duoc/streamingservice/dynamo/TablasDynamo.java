package cl.duoc.streamingservice.dynamo;

import cl.duoc.streamingservice.config.StreamingProperties;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/** Crea al arrancar las dos tablas propias del servicio (la KCL crea las suyas). Capacidad bajo demanda. */
@Component
@ConditionalOnProperty(name = "streaming.habilitado", havingValue = "true")
public class TablasDynamo {

    private static final Logger log = LoggerFactory.getLogger(TablasDynamo.class);
    static final String ATRIBUTO_CLAVE = "clave";

    private final DynamoDbClient dynamo;
    private final StreamingProperties props;

    public TablasDynamo(DynamoDbClient dynamo, StreamingProperties props) {
        this.dynamo = dynamo;
        this.props = props;
    }

    @PostConstruct
    public void asegurarTablas() {
        asegurar(props.tablas().contadores());
        asegurar(props.tablas().procesados());
    }

    private void asegurar(String tabla) {
        DescribeTableRequest describir = DescribeTableRequest.builder().tableName(tabla).build();
        try {
            dynamo.describeTable(describir);
            log.info("Tabla DynamoDB lista: {}", tabla);
        } catch (ResourceNotFoundException e) {
            dynamo.createTable(CreateTableRequest.builder()
                    .tableName(tabla)
                    .billingMode(BillingMode.PAY_PER_REQUEST)
                    .attributeDefinitions(AttributeDefinition.builder()
                            .attributeName(ATRIBUTO_CLAVE).attributeType(ScalarAttributeType.S).build())
                    .keySchema(KeySchemaElement.builder()
                            .attributeName(ATRIBUTO_CLAVE).keyType(KeyType.HASH).build())
                    .build());
            dynamo.waiter().waitUntilTableExists(describir);
            log.info("Tabla DynamoDB creada: {}", tabla);
        }
    }
}
