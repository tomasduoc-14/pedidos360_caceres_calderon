package cl.duoc.adminservice.service;

import cl.duoc.adminservice.config.StreamAdminProperties;
import cl.duoc.adminservice.dto.StreamDtos.ActualizarRetencionRequest;
import cl.duoc.adminservice.dto.StreamDtos.AplicacionEstado;
import cl.duoc.adminservice.dto.StreamDtos.AplicacionResumen;
import cl.duoc.adminservice.dto.StreamDtos.CambioRetencion;
import cl.duoc.adminservice.dto.StreamDtos.CambioShards;
import cl.duoc.adminservice.dto.StreamDtos.CrearStreamRequest;
import cl.duoc.adminservice.dto.StreamDtos.LagStream;
import cl.duoc.adminservice.dto.StreamDtos.PuntoLag;
import cl.duoc.adminservice.dto.StreamDtos.ShardConsumo;
import cl.duoc.adminservice.dto.StreamDtos.ShardInfo;
import cl.duoc.adminservice.dto.StreamDtos.StreamCreado;
import cl.duoc.adminservice.dto.StreamDtos.StreamDetalle;
import cl.duoc.adminservice.dto.StreamDtos.StreamResumen;
import cl.duoc.adminservice.error.OperacionNoPermitidaException;
import cl.duoc.adminservice.error.RecursoNoEncontradoException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Datapoint;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricStatisticsRequest;
import software.amazon.awssdk.services.cloudwatch.model.Statistic;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DecreaseStreamRetentionPeriodRequest;
import software.amazon.awssdk.services.kinesis.model.DeleteStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.IncreaseStreamRetentionPeriodRequest;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.ListStreamsRequest;
import software.amazon.awssdk.services.kinesis.model.ListStreamsResponse;
import software.amazon.awssdk.services.kinesis.model.ScalingType;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StreamDescriptionSummary;
import software.amazon.awssdk.services.kinesis.model.StreamMode;
import software.amazon.awssdk.services.kinesis.model.StreamModeDetails;
import software.amazon.awssdk.services.kinesis.model.UpdateShardCountRequest;
import software.amazon.awssdk.services.kinesis.model.UpdateShardCountResponse;

/**
 * Toda la logica de administracion de streams de Kinesis y de inspeccion de las aplicaciones
 * consumidoras (KCL). Los controladores solo llaman a estos metodos de alto nivel.
 *
 * Como se "ve" una aplicacion consumidora: KCL guarda en DynamoDB, en una tabla con el nombre de la
 * aplicacion, una fila por shard con el dueno actual (leaseOwner) y el checkpoint. Esa tabla es la
 * fuente de verdad de "que shards tiene cada instancia" y "hasta donde va cada shard".
 */
@Service
public class StreamAdminService {

    private static final Logger log = LoggerFactory.getLogger(StreamAdminService.class);

    private static final String ACCESO_DENEGADO = "AccessDeniedException";
    private static final String METRICA_EDAD_ITERADOR = "GetRecords.IteratorAgeMilliseconds";
    private static final int ESPERAS_MAX = 30;
    private static final long ESPERA_MS = 2000;

    private final KinesisClient kinesis;
    private final DynamoDbClient dynamo;
    private final CloudWatchClient cloudWatch;
    private final StreamAdminProperties props;

    public StreamAdminService(KinesisClient kinesis, DynamoDbClient dynamo, CloudWatchClient cloudWatch,
                              StreamAdminProperties props) {
        this.kinesis = kinesis;
        this.dynamo = dynamo;
        this.cloudWatch = cloudWatch;
        this.props = props;
    }

    // ---------------------------------------------------------------- streams

    public StreamCreado crearStream(CrearStreamRequest req) {
        int shards = req.shards() != null ? req.shards() : props.porDefecto().shards();
        int retencion = req.retencionHoras() != null ? req.retencionHoras() : props.porDefecto().retencionHoras();
        String modo = req.modo() != null ? req.modo() : "PROVISIONED";

        CreateStreamRequest.Builder crear = CreateStreamRequest.builder().streamName(req.nombre());
        if ("ON_DEMAND".equals(modo)) {
            crear.streamModeDetails(StreamModeDetails.builder().streamMode(StreamMode.ON_DEMAND).build());
        } else {
            crear.shardCount(shards)
                    .streamModeDetails(StreamModeDetails.builder().streamMode(StreamMode.PROVISIONED).build());
        }
        kinesis.createStream(crear.build());
        log.info("STREAM CREADO nombre={} modo={} shards={} retencionSolicitadaHoras={}",
                req.nombre(), modo, shards, retencion);

        List<String> advertencias = new ArrayList<>();
        if (retencion > 24) {
            // La retencion solo se puede cambiar cuando el stream ya esta ACTIVE
            esperarActivo(req.nombre());
            try {
                kinesis.increaseStreamRetentionPeriod(IncreaseStreamRetentionPeriodRequest.builder()
                        .streamName(req.nombre()).retentionPeriodHours(retencion).build());
            } catch (KinesisException e) {
                if (!esAccesoDenegado(e)) {
                    throw e;
                }
                String aviso = "El entorno no permite subir la retencion a " + retencion
                        + " h (politica del laboratorio): el stream queda con 24 h";
                log.warn("RETENCION NO APLICADA stream={} causa={}", req.nombre(), aviso);
                advertencias.add(aviso);
            }
        }
        return new StreamCreado(resumen(req.nombre()), advertencias);
    }

    public List<StreamResumen> listarStreams() {
        List<String> nombres = new ArrayList<>();
        String desde = null;
        boolean hayMas = true;
        while (hayMas) {
            ListStreamsRequest.Builder b = ListStreamsRequest.builder();
            if (desde != null) {
                b.exclusiveStartStreamName(desde);
            }
            ListStreamsResponse r = kinesis.listStreams(b.build());
            nombres.addAll(r.streamNames());
            hayMas = Boolean.TRUE.equals(r.hasMoreStreams()) && !r.streamNames().isEmpty();
            if (hayMas) {
                desde = r.streamNames().get(r.streamNames().size() - 1);
            }
        }
        List<StreamResumen> resultado = new ArrayList<>();
        for (String nombre : nombres) {
            resultado.add(resumen(nombre));
        }
        return resultado;
    }

    public StreamDetalle describirStream(String nombre) {
        return new StreamDetalle(resumen(nombre), listarShards(nombre));
    }

    public CambioShards actualizarShards(String nombre, int shardsObjetivo) {
        StreamResumen actual = resumen(nombre);
        if ("ON_DEMAND".equals(actual.modo())) {
            throw new IllegalArgumentException("El stream es ON_DEMAND: AWS administra los shards");
        }
        if (actual.shards() == shardsObjetivo) {
            throw new IllegalArgumentException("El stream ya tiene " + shardsObjetivo + " shards");
        }
        UpdateShardCountResponse r = kinesis.updateShardCount(UpdateShardCountRequest.builder()
                .streamName(nombre)
                .targetShardCount(shardsObjetivo)
                .scalingType(ScalingType.UNIFORM_SCALING)
                .build());
        log.info("SHARDS ACTUALIZADOS stream={} {} -> {}", nombre, r.currentShardCount(), r.targetShardCount());
        return new CambioShards(nombre, r.currentShardCount(), r.targetShardCount(),
                "Kinesis reparte el cambio en segundo plano: el stream pasa a UPDATING y luego a ACTIVE. "
                        + "Los shards antiguos se cierran y los consumidores toman los nuevos automaticamente.");
    }

    public CambioRetencion actualizarRetencion(String nombre, int horas) {
        int actual = resumen(nombre).retencionHoras();
        if (actual == horas) {
            return new CambioRetencion(nombre, actual, actual);
        }
        try {
            if (horas > actual) {
                kinesis.increaseStreamRetentionPeriod(IncreaseStreamRetentionPeriodRequest.builder()
                        .streamName(nombre).retentionPeriodHours(horas).build());
            } else {
                kinesis.decreaseStreamRetentionPeriod(DecreaseStreamRetentionPeriodRequest.builder()
                        .streamName(nombre).retentionPeriodHours(horas).build());
            }
        } catch (KinesisException e) {
            if (esAccesoDenegado(e)) {
                throw new OperacionNoPermitidaException("El entorno no permite cambiar la retencion de '" + nombre
                        + "' de " + actual + " h a " + horas + " h (politica del laboratorio)");
            }
            throw e;
        }
        log.info("RETENCION ACTUALIZADA stream={} {} h -> {} h", nombre, actual, horas);
        return new CambioRetencion(nombre, actual, horas);
    }

    public void eliminarStream(String nombre) {
        conStream(nombre, () -> kinesis.deleteStream(DeleteStreamRequest.builder()
                .streamName(nombre).enforceConsumerDeletion(true).build()));
        log.info("STREAM ELIMINADO nombre={}", nombre);
    }

    /** Atraso a nivel de stream: edad maxima del registro leido (CloudWatch, ultimos N minutos). */
    public LagStream lagDelStream(String nombre) {
        resumen(nombre); // 404 si no existe
        int ventana = props.ventanaLagMinutos();
        Instant ahora = Instant.now();
        List<Datapoint> datos = cloudWatch.getMetricStatistics(GetMetricStatisticsRequest.builder()
                .namespace("AWS/Kinesis")
                .metricName(METRICA_EDAD_ITERADOR)
                .dimensions(Dimension.builder().name("StreamName").value(nombre).build())
                .startTime(ahora.minus(ventana, ChronoUnit.MINUTES))
                .endTime(ahora)
                .period(60)
                .statistics(Statistic.MAXIMUM)
                .build()).datapoints();
        List<PuntoLag> puntos = datos.stream()
                .sorted(Comparator.comparing(Datapoint::timestamp))
                .map(d -> new PuntoLag(d.timestamp().toString(), d.maximum()))
                .toList();
        Double maximo = puntos.isEmpty() ? null : puntos.stream().mapToDouble(PuntoLag::maximoMs).max().getAsDouble();
        return new LagStream(nombre, ventana, maximo, puntos);
    }

    // ----------------------------------------------- aplicaciones consumidoras

    public List<AplicacionResumen> listarAplicaciones() {
        List<AplicacionResumen> resultado = new ArrayList<>();
        for (StreamAdminProperties.Aplicacion app : props.aplicaciones()) {
            try {
                List<ShardConsumo> shards = combinar(idsShardsAbiertos(app.stream()), leases(app.nombre()));
                resultado.add(resumenApp(app, shards));
            } catch (RecursoNoEncontradoException e) {
                // La aplicacion aun no se ha iniciado: no existe su tabla de coordinacion
                resultado.add(new AplicacionResumen(app.nombre(), app.stream(), app.nombre(), 0, 0, List.of()));
            }
        }
        return resultado;
    }

    /** Shards asignados, checkpoint y atraso (ms detras del final del stream) de cada shard. */
    public AplicacionEstado estadoAplicacion(String aplicacion) {
        StreamAdminProperties.Aplicacion app = props.aplicaciones().stream()
                .filter(a -> a.nombre().equals(aplicacion)).findFirst()
                .orElseThrow(() -> new RecursoNoEncontradoException(
                        "La aplicacion '" + aplicacion + "' no esta configurada en kinesis-admin.aplicaciones"));
        List<ShardConsumo> base = combinar(idsShardsAbiertos(app.stream()), leases(app.nombre()));
        List<ShardConsumo> conLag = new ArrayList<>();
        for (ShardConsumo s : base) {
            conLag.add(s.conLag(lagDelShard(app.stream(), s.shardId(), s.checkpoint())));
        }
        return new AplicacionEstado(app.nombre(), app.stream(), app.nombre(), instancias(conLag), conLag);
    }

    // ----------------------------------------------------------------- ayudas

    /**
     * Cruza los shards abiertos del stream con las filas de la tabla de coordinacion de KCL.
     * La tabla tambien guarda filas de control (Leader, Migration3.0, metricas de los workers):
     * solo se consideran las filas cuya clave contiene "shardId-".
     */
    static List<ShardConsumo> combinar(List<String> shardsAbiertos, List<Map<String, AttributeValue>> items) {
        Map<String, Map<String, AttributeValue>> porShard = new LinkedHashMap<>();
        for (Map<String, AttributeValue> item : items) {
            String clave = texto(item, "leaseKey");
            if (clave != null && clave.contains("shardId-")) {
                porShard.put(clave.substring(clave.indexOf("shardId-")), item);
            }
        }
        List<ShardConsumo> resultado = new ArrayList<>();
        for (String shardId : shardsAbiertos) {
            Map<String, AttributeValue> item = porShard.get(shardId);
            if (item == null) {
                resultado.add(new ShardConsumo(shardId, null, null, null, null, null, "SIN_ASIGNAR"));
                continue;
            }
            String dueno = texto(item, "leaseOwner");
            resultado.add(new ShardConsumo(shardId, dueno, texto(item, "checkpoint"),
                    numero(item, "checkpointSubSequenceNumber"), numero(item, "leaseCounter"), null,
                    dueno == null ? "SIN_ASIGNAR" : "ASIGNADO"));
        }
        return resultado;
    }

    private static AplicacionResumen resumenApp(StreamAdminProperties.Aplicacion app, List<ShardConsumo> shards) {
        int asignados = (int) shards.stream().filter(s -> "ASIGNADO".equals(s.estado())).count();
        return new AplicacionResumen(app.nombre(), app.stream(), app.nombre(), asignados,
                shards.size() - asignados, instancias(shards));
    }

    private static List<String> instancias(List<ShardConsumo> shards) {
        TreeSet<String> duenos = new TreeSet<>();
        for (ShardConsumo s : shards) {
            if (s.dueno() != null) {
                duenos.add(s.dueno());
            }
        }
        return new ArrayList<>(duenos);
    }

    private static String texto(Map<String, AttributeValue> item, String campo) {
        AttributeValue v = item.get(campo);
        return v == null ? null : v.s();
    }

    private static Long numero(Map<String, AttributeValue> item, String campo) {
        AttributeValue v = item.get(campo);
        if (v == null || v.n() == null) {
            return null;
        }
        return Long.parseLong(v.n());
    }

    private List<Map<String, AttributeValue>> leases(String tabla) {
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> desde = null;
        try {
            do {
                ScanRequest.Builder b = ScanRequest.builder().tableName(tabla);
                if (desde != null) {
                    b.exclusiveStartKey(desde);
                }
                ScanResponse r = dynamo.scan(b.build());
                items.addAll(r.items());
                desde = r.hasLastEvaluatedKey() ? r.lastEvaluatedKey() : null;
            } while (desde != null && !desde.isEmpty());
        } catch (software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException e) {
            throw new RecursoNoEncontradoException("La aplicacion '" + tabla
                    + "' aun no se ha iniciado: no existe su tabla de coordinacion en DynamoDB");
        }
        return items;
    }

    /**
     * Atraso del shard: se lee 1 registro despues del checkpoint y Kinesis informa en la misma respuesta
     * cuantos milisegundos falta para llegar al final del shard (MillisBehindLatest).
     */
    private Long lagDelShard(String stream, String shardId, String checkpoint) {
        if (checkpoint == null || checkpoint.isBlank()) {
            return null;
        }
        if ("SHARD_END".equals(checkpoint)) {
            return 0L;
        }
        GetShardIteratorRequest.Builder b = GetShardIteratorRequest.builder().streamName(stream).shardId(shardId);
        if ("TRIM_HORIZON".equals(checkpoint)) {
            b.shardIteratorType(ShardIteratorType.TRIM_HORIZON);
        } else if ("LATEST".equals(checkpoint)) {
            b.shardIteratorType(ShardIteratorType.LATEST);
        } else {
            b.shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER).startingSequenceNumber(checkpoint);
        }
        String iterador = kinesis.getShardIterator(b.build()).shardIterator();
        return kinesis.getRecords(GetRecordsRequest.builder().shardIterator(iterador).limit(1).build())
                .millisBehindLatest();
    }

    private List<String> idsShardsAbiertos(String stream) {
        return listarShards(stream).stream().filter(ShardInfo::abierto).map(ShardInfo::shardId).toList();
    }

    private List<ShardInfo> listarShards(String stream) {
        List<ShardInfo> shards = new ArrayList<>();
        String token = null;
        do {
            ListShardsRequest peticion = token == null
                    ? ListShardsRequest.builder().streamName(stream).build()
                    : ListShardsRequest.builder().nextToken(token).build();
            ListShardsResponse r = conStream(stream, () -> kinesis.listShards(peticion));
            for (Shard s : r.shards()) {
                shards.add(new ShardInfo(s.shardId(),
                        s.hashKeyRange().startingHashKey(), s.hashKeyRange().endingHashKey(),
                        s.sequenceNumberRange().startingSequenceNumber(), s.parentShardId(),
                        s.sequenceNumberRange().endingSequenceNumber() == null));
            }
            token = r.nextToken();
        } while (token != null);
        return shards;
    }

    private StreamResumen resumen(String nombre) {
        StreamDescriptionSummary d = conStream(nombre, () -> kinesis.describeStreamSummary(
                DescribeStreamSummaryRequest.builder().streamName(nombre).build()).streamDescriptionSummary());
        String modo = d.streamModeDetails() == null ? "PROVISIONED" : d.streamModeDetails().streamModeAsString();
        return new StreamResumen(d.streamName(), d.streamStatusAsString(), modo,
                d.openShardCount() == null ? 0 : d.openShardCount(),
                d.retentionPeriodHours() == null ? 0 : d.retentionPeriodHours(), d.streamARN());
    }

    private void esperarActivo(String nombre) {
        for (int i = 0; i < ESPERAS_MAX; i++) {
            if ("ACTIVE".equals(resumen(nombre).estado())) {
                return;
            }
            try {
                Thread.sleep(ESPERA_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        log.warn("El stream {} no llego a ACTIVE a tiempo", nombre);
    }

    private <T> T conStream(String nombre, Supplier<T> operacion) {
        try {
            return operacion.get();
        } catch (software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException e) {
            throw new RecursoNoEncontradoException("El stream '" + nombre + "' no existe");
        }
    }

    private static boolean esAccesoDenegado(AwsServiceException e) {
        return e.awsErrorDetails() != null && ACCESO_DENEGADO.equals(e.awsErrorDetails().errorCode());
    }
}
