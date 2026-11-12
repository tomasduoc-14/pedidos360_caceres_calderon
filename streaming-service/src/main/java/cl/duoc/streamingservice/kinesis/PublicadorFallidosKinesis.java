package cl.duoc.streamingservice.kinesis;

import cl.duoc.streamingservice.config.StreamingProperties;
import cl.duoc.streamingservice.evento.EventoFallido;
import cl.duoc.streamingservice.proceso.ProcesadorEventos;
import cl.duoc.streamingservice.proceso.PublicadorFallidos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import tools.jackson.databind.json.JsonMapper;

/** Escribe los eventos que no se pudieron procesar en el stream de mensajes fallidos. */
@Component
@ConditionalOnProperty(name = "streaming.habilitado", havingValue = "true")
public class PublicadorFallidosKinesis implements PublicadorFallidos {

    private static final Logger log = LoggerFactory.getLogger(PublicadorFallidosKinesis.class);
    private static final int INTENTOS = 3;

    private final KinesisClient kinesis;
    private final JsonMapper json;
    private final String stream;

    public PublicadorFallidosKinesis(KinesisClient kinesis, JsonMapper json, StreamingProperties props) {
        this.kinesis = kinesis;
        this.json = json;
        this.stream = props.streams().fallidos().nombre();
    }

    @Override
    public boolean enviar(EventoFallido evento) {
        String cuerpo;
        try {
            cuerpo = json.writeValueAsString(evento);
        } catch (RuntimeException e) {
            log.error("No se pudo serializar el evento fallido eventoId={} causa={}", evento.eventoId(), e.getMessage());
            return false;
        }
        String clave = evento.eventoId() == null ? "sin-id" : evento.eventoId();
        for (int intento = 1; intento <= INTENTOS; intento++) {
            try {
                kinesis.putRecord(PutRecordRequest.builder()
                        .streamName(stream)
                        .partitionKey(clave)
                        .data(SdkBytes.fromUtf8String(cuerpo))
                        .build());
                log.warn("ENVIADO AL STREAM DE FALLIDOS stream={} app={} eventoId={} causa={}",
                        stream, evento.aplicacion(), evento.eventoId(), evento.causa());
                return true;
            } catch (SdkException e) {
                log.warn("No se pudo escribir en {} eventoId={} intento={}/{} causa={}",
                        stream, evento.eventoId(), intento, INTENTOS, e.getMessage());
                ProcesadorEventos.dormir(300L * intento);
            }
        }
        return false;
    }
}
