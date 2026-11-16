package cl.duoc.adminservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import cl.duoc.adminservice.config.StreamAdminProperties;
import cl.duoc.adminservice.dto.StreamDtos.CrearStreamRequest;
import cl.duoc.adminservice.dto.StreamDtos.ShardConsumo;
import cl.duoc.adminservice.dto.StreamDtos.StreamCreado;
import cl.duoc.adminservice.error.OperacionNoPermitidaException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.IncreaseStreamRetentionPeriodRequest;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.StreamDescriptionSummary;
import software.amazon.awssdk.services.kinesis.model.StreamMode;
import software.amazon.awssdk.services.kinesis.model.StreamModeDetails;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;
import software.amazon.awssdk.services.kinesis.model.UpdateShardCountRequest;

class StreamAdminServiceTest {

    private KinesisClient kinesis;
    private StreamAdminService servicio;

    @BeforeEach
    void preparar() {
        kinesis = mock(KinesisClient.class);
        StreamAdminProperties props = new StreamAdminProperties("us-east-1",
                new StreamAdminProperties.PorDefecto(3, 24),
                List.of(new StreamAdminProperties.Aplicacion("analitica-app", "pedidos-estado")), 10);
        servicio = new StreamAdminService(kinesis, mock(DynamoDbClient.class), mock(CloudWatchClient.class), props);
    }

    private static DescribeStreamSummaryResponse resumen(StreamMode modo, int shards, int horas) {
        return DescribeStreamSummaryResponse.builder().streamDescriptionSummary(
                StreamDescriptionSummary.builder()
                        .streamName("pedidos-estado")
                        .streamStatus(StreamStatus.ACTIVE)
                        .streamModeDetails(StreamModeDetails.builder().streamMode(modo).build())
                        .openShardCount(shards)
                        .retentionPeriodHours(horas)
                        .streamARN("arn:aws:kinesis:us-east-1:000000000000:stream/pedidos-estado")
                        .build()).build();
    }

    private static KinesisException accesoDenegado() {
        return (KinesisException) KinesisException.builder()
                .awsErrorDetails(AwsErrorDetails.builder().errorCode("AccessDeniedException").build())
                .statusCode(400)
                .message("explicit deny")
                .build();
    }

    @Test
    void creaStreamConLosShardsPorDefectoYSinTocarLaRetencion() {
        when(kinesis.describeStreamSummary(any(DescribeStreamSummaryRequest.class)))
                .thenReturn(resumen(StreamMode.PROVISIONED, 3, 24));

        StreamCreado creado = servicio.crearStream(new CrearStreamRequest("pedidos-estado", null, null, null));

        verify(kinesis).createStream(any(CreateStreamRequest.class));
        verify(kinesis, never()).increaseStreamRetentionPeriod(any(IncreaseStreamRetentionPeriodRequest.class));
        assertThat(creado.advertencias()).isEmpty();
        assertThat(creado.stream().shards()).isEqualTo(3);
    }

    @Test
    void siElLaboratorioBloqueaLaRetencionElStreamSeCreaConUnaAdvertencia() {
        when(kinesis.describeStreamSummary(any(DescribeStreamSummaryRequest.class)))
                .thenReturn(resumen(StreamMode.PROVISIONED, 3, 24));
        when(kinesis.increaseStreamRetentionPeriod(any(IncreaseStreamRetentionPeriodRequest.class)))
                .thenThrow(accesoDenegado());

        StreamCreado creado = servicio.crearStream(new CrearStreamRequest("pedidos-estado", 3, 72, "PROVISIONED"));

        assertThat(creado.advertencias()).hasSize(1);
        assertThat(creado.advertencias().get(0)).contains("72");
    }

    @Test
    void cambiarLaRetencionBloqueadaDaOperacionNoPermitida() {
        when(kinesis.describeStreamSummary(any(DescribeStreamSummaryRequest.class)))
                .thenReturn(resumen(StreamMode.PROVISIONED, 3, 24));
        when(kinesis.increaseStreamRetentionPeriod(any(IncreaseStreamRetentionPeriodRequest.class)))
                .thenThrow(accesoDenegado());

        assertThatThrownBy(() -> servicio.actualizarRetencion("pedidos-estado", 72))
                .isInstanceOf(OperacionNoPermitidaException.class);
    }

    @Test
    void noSePuedenCambiarLosShardsDeUnStreamOnDemand() {
        when(kinesis.describeStreamSummary(any(DescribeStreamSummaryRequest.class)))
                .thenReturn(resumen(StreamMode.ON_DEMAND, 4, 24));

        assertThatThrownBy(() -> servicio.actualizarShards("pedidos-estado", 6))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ON_DEMAND");
        verify(kinesis, never()).updateShardCount(any(UpdateShardCountRequest.class));
    }

    @Test
    void pedirLosMismosShardsQueYaTieneEsUnaPeticionInvalida() {
        when(kinesis.describeStreamSummary(any(DescribeStreamSummaryRequest.class)))
                .thenReturn(resumen(StreamMode.PROVISIONED, 3, 24));

        assertThatThrownBy(() -> servicio.actualizarShards("pedidos-estado", 3))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void combinaShardsConLeasesIgnorandoLasFilasDeControlDeKcl() {
        Map<String, AttributeValue> lease0 = Map.of(
                "leaseKey", AttributeValue.builder().s("shardId-000000000000").build(),
                "leaseOwner", AttributeValue.builder().s("PC:abc").build(),
                "checkpoint", AttributeValue.builder().s("4968028720458").build(),
                "leaseCounter", AttributeValue.builder().n("7").build());
        Map<String, AttributeValue> filaLider = Map.of(
                "leaseKey", AttributeValue.builder().s("Leader").build());
        Map<String, AttributeValue> filaWorker = Map.of(
                "leaseKey", AttributeValue.builder().s("PC:abc").build());

        List<ShardConsumo> r = StreamAdminService.combinar(
                List.of("shardId-000000000000", "shardId-000000000001"),
                List.of(lease0, filaLider, filaWorker));

        assertThat(r).hasSize(2);
        assertThat(r.get(0).estado()).isEqualTo("ASIGNADO");
        assertThat(r.get(0).dueno()).isEqualTo("PC:abc");
        assertThat(r.get(0).checkpoint()).isEqualTo("4968028720458");
        assertThat(r.get(0).contadorLease()).isEqualTo(7L);
        assertThat(r.get(1).estado()).isEqualTo("SIN_ASIGNAR");
        assertThat(r.get(1).dueno()).isNull();
    }
}
