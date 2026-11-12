package cl.duoc.streamingservice.kcl;

import cl.duoc.streamingservice.config.StreamingProperties;
import java.net.InetAddress;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.metrics.MetricsConfig;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.processor.SingleStreamTracker;
import software.amazon.kinesis.processor.StreamTracker;
import software.amazon.kinesis.retrieval.RetrievalConfig;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

/**
 * Una aplicacion consumidora (consumer group): un Scheduler de la KCL con nombre propio.
 * El nombre de la aplicacion es el de su tabla de coordinacion en DynamoDB: todas las instancias
 * con el mismo nombre se reparten los shards, y cada nombre distinto lleva su propio avance.
 */
public class AplicacionConsumidora {

    private static final Logger log = LoggerFactory.getLogger(AplicacionConsumidora.class);

    private final String aplicacion;
    private final String stream;
    private final ShardRecordProcessorFactory fabrica;
    private final KinesisAsyncClient kinesis;
    private final DynamoDbAsyncClient dynamo;
    private final CloudWatchAsyncClient cloudWatch;
    private final StreamingProperties.Kcl kcl;

    private Scheduler scheduler;

    public AplicacionConsumidora(String aplicacion, String stream, ShardRecordProcessorFactory fabrica,
                                 KinesisAsyncClient kinesis, DynamoDbAsyncClient dynamo,
                                 CloudWatchAsyncClient cloudWatch, StreamingProperties.Kcl kcl) {
        this.aplicacion = aplicacion;
        this.stream = stream;
        this.fabrica = fabrica;
        this.kinesis = kinesis;
        this.dynamo = dynamo;
        this.cloudWatch = cloudWatch;
        this.kcl = kcl;
    }

    public synchronized void iniciar() {
        if (scheduler != null) {
            return;
        }
        String trabajador = nombreTrabajador();
        InitialPositionInStream posicion =
                InitialPositionInStream.valueOf(kcl.posicionInicial().toUpperCase(Locale.ROOT));
        StreamTracker tracker = new SingleStreamTracker(stream, InitialPositionInStreamExtended.newInitialPosition(posicion));

        ConfigsBuilder configs = new ConfigsBuilder(tracker, aplicacion, kinesis, dynamo, cloudWatch, trabajador, fabrica);

        // Lectura por polling (GetRecords). Sin esto la KCL usaria "enhanced fan-out", que cobra aparte.
        RetrievalConfig recuperacion = configs.retrievalConfig();
        PollingConfig polling = new PollingConfig(stream, kinesis);
        polling.idleTimeBetweenReadsInMillis(kcl.esperaEntreLecturasMs());
        recuperacion.retrievalSpecificConfig(polling);

        MetricsConfig metricas = configs.metricsConfig();
        metricas.metricsLevel(MetricsLevel.valueOf(kcl.nivelMetricas().toUpperCase(Locale.ROOT)));

        scheduler = new Scheduler(
                configs.checkpointConfig(),
                configs.coordinatorConfig(),
                configs.leaseManagementConfig(),
                configs.lifecycleConfig(),
                metricas,
                configs.processorConfig(),
                recuperacion);

        Thread hilo = new Thread(scheduler, "kcl-" + aplicacion);
        hilo.setDaemon(true);
        hilo.start();
        log.info("APLICACION CONSUMIDORA INICIADA app={} stream={} worker={} posicionInicial={}",
                aplicacion, stream, trabajador, posicion);
    }

    public synchronized void detener() {
        if (scheduler == null) {
            return;
        }
        log.info("Deteniendo aplicacion consumidora app={} (apagado gracioso)", aplicacion);
        try {
            CompletableFuture<Boolean> apagado = scheduler.startGracefulShutdown();
            apagado.get(kcl.apagadoGraciosoSegundos(), TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.warn("Apagado gracioso incompleto app={} causa={}", aplicacion, e.toString());
            scheduler.shutdown();
        }
        scheduler = null;
    }

    public String aplicacion() {
        return aplicacion;
    }

    private static String nombreTrabajador() {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            host = "worker";
        }
        return host + ":" + UUID.randomUUID();
    }
}
