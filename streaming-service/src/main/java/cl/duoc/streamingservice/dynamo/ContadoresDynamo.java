package cl.duoc.streamingservice.dynamo;

import cl.duoc.streamingservice.config.StreamingProperties;
import cl.duoc.streamingservice.dominio.ContadoresRepositorio;
import java.util.Map;
import java.util.TreeMap;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

/** Totales en DynamoDB: el incremento es atomico, asi que varias instancias suman sin pisarse. */
@Component
@ConditionalOnProperty(name = "streaming.habilitado", havingValue = "true")
public class ContadoresDynamo implements ContadoresRepositorio {

    private static final String ATRIBUTO_TOTAL = "cantidad";

    private final DynamoDbClient dynamo;
    private final String tabla;

    public ContadoresDynamo(DynamoDbClient dynamo, StreamingProperties props) {
        this.dynamo = dynamo;
        this.tabla = props.tablas().contadores();
    }

    @Override
    public void incrementar(String clave, long delta) {
        dynamo.updateItem(UpdateItemRequest.builder()
                .tableName(tabla)
                .key(Map.of(TablasDynamo.ATRIBUTO_CLAVE, AttributeValue.builder().s(clave).build()))
                .updateExpression("ADD " + ATRIBUTO_TOTAL + " :delta")
                .expressionAttributeValues(Map.of(":delta", AttributeValue.builder().n(String.valueOf(delta)).build()))
                .build());
    }

    @Override
    public Map<String, Long> listar(String prefijo) {
        ScanRequest peticion = ScanRequest.builder()
                .tableName(tabla)
                .filterExpression("begins_with(" + TablasDynamo.ATRIBUTO_CLAVE + ", :prefijo)")
                .expressionAttributeValues(Map.of(":prefijo", AttributeValue.builder().s(prefijo).build()))
                .build();
        Map<String, Long> resultado = new TreeMap<>();
        for (Map<String, AttributeValue> item : dynamo.scanPaginator(peticion).items()) {
            String clave = item.get(TablasDynamo.ATRIBUTO_CLAVE).s();
            resultado.put(clave.substring(prefijo.length()), Long.parseLong(item.get(ATRIBUTO_TOTAL).n()));
        }
        return resultado;
    }
}
