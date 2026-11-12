package cl.duoc.streamingservice.kcl;

import cl.duoc.streamingservice.proceso.Origen;
import cl.duoc.streamingservice.proceso.ProcesadorEventos;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.kinesis.exceptions.KinesisClientLibException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

/**
 * La KCL crea una instancia por cada shard que esta instancia de la aplicacion tiene asignado.
 * Aqui solo se hace el trabajo de infraestructura: leer el lote, entregar cada registro al
 * procesador de eventos y registrar el avance (checkpoint) al terminar el lote.
 */
public class ProcesadorShard<T> implements ShardRecordProcessor {

    private static final Logger log = LoggerFactory.getLogger(ProcesadorShard.class);
    private static final int INTENTOS_CHECKPOINT = 3;

    private final String aplicacion;
    private final ProcesadorEventos<T> procesador;
    private String shardId = "desconocido";

    public ProcesadorShard(String aplicacion, ProcesadorEventos<T> procesador) {
        this.aplicacion = aplicacion;
        this.procesador = procesador;
    }

    @Override
    public void initialize(InitializationInput entrada) {
        shardId = entrada.shardId();
        log.info("SHARD ASIGNADO app={} shard={} desde={}", aplicacion, shardId, entrada.extendedSequenceNumber());
    }

    @Override
    public void processRecords(ProcessRecordsInput entrada) {
        List<KinesisClientRecord> registros = entrada.records();
        if (registros == null || registros.isEmpty()) {
            return;
        }
        for (KinesisClientRecord registro : registros) {
            Origen origen = new Origen(shardId, registro.sequenceNumber(), registro.partitionKey());
            try {
                procesador.procesar(texto(registro.data()), origen);
            } catch (RuntimeException e) {
                // Ultima red de seguridad: un evento defectuoso nunca debe detener la lectura del shard.
                log.error("ERROR INESPERADO app={} shard={} sequenceNumber={} causa={}",
                        aplicacion, shardId, registro.sequenceNumber(), e.toString());
            }
        }
        log.debug("LOTE app={} shard={} registros={} retraso={}ms",
                aplicacion, shardId, registros.size(), entrada.millisBehindLatest());
        checkpoint(entrada.checkpointer(), "fin de lote");
    }

    @Override
    public void leaseLost(LeaseLostInput entrada) {
        log.warn("SHARD PERDIDO app={} shard={} (otra instancia lo tomo)", aplicacion, shardId);
    }

    @Override
    public void shardEnded(ShardEndedInput entrada) {
        // Obligatorio: sin este checkpoint la KCL no pasa a los shards hijos tras un reshard.
        log.info("SHARD CERRADO app={} shard={}", aplicacion, shardId);
        checkpoint(entrada.checkpointer(), "shard cerrado");
    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput entrada) {
        log.info("APAGADO SOLICITADO app={} shard={}", aplicacion, shardId);
        checkpoint(entrada.checkpointer(), "apagado");
    }

    private void checkpoint(RecordProcessorCheckpointer checkpointer, String motivo) {
        for (int intento = 1; intento <= INTENTOS_CHECKPOINT; intento++) {
            try {
                checkpointer.checkpoint();
                log.debug("CHECKPOINT app={} shard={} motivo={}", aplicacion, shardId, motivo);
                return;
            } catch (ThrottlingException e) {
                log.warn("CHECKPOINT limitado app={} shard={} intento={}/{}",
                        aplicacion, shardId, intento, INTENTOS_CHECKPOINT);
                ProcesadorEventos.dormir(200L * intento);
            } catch (ShutdownException e) {
                log.warn("CHECKPOINT omitido app={} shard={}: este worker ya no es dueno del shard", aplicacion, shardId);
                return;
            } catch (KinesisClientLibException e) {
                log.error("CHECKPOINT fallido app={} shard={} causa={}", aplicacion, shardId, e.toString());
                return;
            }
        }
    }

    private static String texto(ByteBuffer datos) {
        ByteBuffer copia = datos.asReadOnlyBuffer();
        byte[] bytes = new byte[copia.remaining()];
        copia.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
