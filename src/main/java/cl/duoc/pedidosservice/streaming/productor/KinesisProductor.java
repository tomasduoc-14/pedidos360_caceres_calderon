package cl.duoc.pedidosservice.streaming.productor;

import cl.duoc.pedidosservice.config.streaming.StreamingProperties;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

/**
 * Envia registros a Kinesis con PutRecords. Un envio por lote puede salir "bien" y aun asi traer
 * registros rechazados (failedRecordCount > 0): se revisa cada resultado y solo los rechazados
 * se reintentan, con espera creciente.
 */
@Component
@ConditionalOnProperty(name = "streaming.habilitado", havingValue = "true")
public class KinesisProductor {

    private static final Logger log = LoggerFactory.getLogger(KinesisProductor.class);

    public record Entrada(String eventoId, String claveParticion, byte[] datos) {
    }

    public record ResultadoEnvio(int enviados, int fallidos) {
    }

    private final KinesisClient kinesis;
    private final StreamingProperties.Productor cfg;

    public KinesisProductor(KinesisClient kinesis, StreamingProperties props) {
        this.kinesis = kinesis;
        this.cfg = props.productor();
    }

    public ResultadoEnvio enviar(String stream, List<Entrada> entradas) {
        List<Entrada> pendientes = new ArrayList<>(entradas);
        int enviados = 0;
        long espera = cfg.esperaInicialMs();

        for (int intento = 1; intento <= cfg.maxIntentos() && !pendientes.isEmpty(); intento++) {
            List<Entrada> rechazados = new ArrayList<>();
            for (List<Entrada> lote : partir(pendientes, cfg.maxRegistrosPorLote())) {
                enviados += enviarLote(stream, lote, intento, rechazados);
            }
            pendientes = rechazados;
            if (!pendientes.isEmpty() && intento < cfg.maxIntentos()) {
                log.warn("Reintentando {} registro(s) rechazado(s): stream={} intento={}/{} espera={}ms",
                        pendientes.size(), stream, intento + 1, cfg.maxIntentos(), espera);
                if (!dormir(espera)) {
                    break;
                }
                espera = Math.min(espera * 2, cfg.esperaMaximaMs());
            }
        }
        return new ResultadoEnvio(enviados, pendientes.size());
    }

    /** Devuelve cuantos registros del lote se aceptaron; los rechazados se agregan a "rechazados". */
    private int enviarLote(String stream, List<Entrada> lote, int intento, List<Entrada> rechazados) {
        List<PutRecordsRequestEntry> registros = lote.stream()
                .map(e -> PutRecordsRequestEntry.builder()
                        .partitionKey(e.claveParticion())
                        .data(SdkBytes.fromByteArray(e.datos()))
                        .build())
                .toList();

        PutRecordsResponse respuesta;
        try {
            respuesta = kinesis.putRecords(PutRecordsRequest.builder()
                    .streamName(stream)
                    .records(registros)
                    .build());
        } catch (SdkException e) {
            // Fallo de toda la llamada (red, permisos, stream inexistente): el lote completo queda pendiente.
            log.warn("PutRecords fallo completo: stream={} intento={} causa={}", stream, intento, e.getMessage());
            rechazados.addAll(lote);
            return 0;
        }

        int aceptados = 0;
        List<PutRecordsResultEntry> resultados = respuesta.records();
        for (int i = 0; i < resultados.size(); i++) {
            PutRecordsResultEntry r = resultados.get(i);
            if (r.errorCode() == null) {
                aceptados++;
                log.debug("Registro aceptado: stream={} shard={} sequenceNumber={}",
                        stream, r.shardId(), r.sequenceNumber());
            } else {
                rechazados.add(lote.get(i));
                log.warn("Registro rechazado: stream={} eventoId={} intento={} codigo={} causa={}",
                        stream, lote.get(i).eventoId(), intento, r.errorCode(), r.errorMessage());
            }
        }
        return aceptados;
    }

    private static List<List<Entrada>> partir(List<Entrada> lista, int tamano) {
        List<List<Entrada>> lotes = new ArrayList<>();
        for (int i = 0; i < lista.size(); i += tamano) {
            lotes.add(lista.subList(i, Math.min(i + tamano, lista.size())));
        }
        return lotes;
    }

    private static boolean dormir(long ms) {
        try {
            Thread.sleep(ms);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
