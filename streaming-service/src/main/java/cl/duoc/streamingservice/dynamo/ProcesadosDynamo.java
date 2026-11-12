package cl.duoc.streamingservice.dynamo;

import cl.duoc.streamingservice.config.StreamingProperties;
import cl.duoc.streamingservice.proceso.RegistroProcesados;
import java.time.Instant;
import java.util.Map;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

/**
 * Registro de eventoId ya procesados, compartido por todas las instancias.
 * La clave incluye la aplicacion: cada aplicacion consumidora procesa el mismo evento una vez.
 */
@Component
@ConditionalOnProperty(name = "streaming.habilitado", havingValue = "true")
public class ProcesadosDynamo implements RegistroProcesados {

    private final DynamoDbClient dynamo;
    private final String tabla;

    public ProcesadosDynamo(DynamoDbClient dynamo, StreamingProperties props) {
        this.dynamo = dynamo;
        this.tabla = props.tablas().procesados();
    }

    @Override
    public boolean yaProcesado(String aplicacion, String eventoId) {
        return dynamo.getItem(GetItemRequest.builder()
                .tableName(tabla)
                .key(clave(aplicacion, eventoId))
                .consistentRead(true)
                .build()).hasItem();
    }

    @Override
    public void marcar(String aplicacion, String eventoId) {
        dynamo.putItem(PutItemRequest.builder()
                .tableName(tabla)
                .item(Map.of(
                        TablasDynamo.ATRIBUTO_CLAVE, AttributeValue.builder().s(aplicacion + "#" + eventoId).build(),
                        "procesadoEn", AttributeValue.builder().s(Instant.now().toString()).build()))
                .build());
    }

    private static Map<String, AttributeValue> clave(String aplicacion, String eventoId) {
        return Map.of(TablasDynamo.ATRIBUTO_CLAVE, AttributeValue.builder().s(aplicacion + "#" + eventoId).build());
    }
}
