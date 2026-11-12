package cl.duoc.pedidosservice.streaming.productor;

import cl.duoc.pedidosservice.config.streaming.StreamingProperties;
import cl.duoc.pedidosservice.streaming.evento.ActividadClienteEvento;
import cl.duoc.pedidosservice.streaming.productor.KinesisProductor.Entrada;
import cl.duoc.pedidosservice.streaming.productor.KinesisProductor.ResultadoEnvio;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

@Component
@ConditionalOnProperty(name = "streaming.habilitado", havingValue = "true")
public class KinesisActividadStreamPublicador implements ActividadStreamPublicador {

    private static final Logger log = LoggerFactory.getLogger(KinesisActividadStreamPublicador.class);

    private final KinesisProductor productor;
    private final JsonMapper json;
    private final StreamingProperties props;

    public KinesisActividadStreamPublicador(KinesisProductor productor, JsonMapper json, StreamingProperties props) {
        this.productor = productor;
        this.json = json;
        this.props = props;
    }

    @Override
    public void publicar(ActividadClienteEvento evento) {
        String stream = props.streams().actividad().nombre();
        try {
            byte[] datos = json.writeValueAsString(evento).getBytes(StandardCharsets.UTF_8);
            // Clave de particion = clienteEmail: la actividad de un cliente se lee en orden.
            ResultadoEnvio resultado = productor.enviar(stream,
                    List.of(new Entrada(evento.eventoId(), evento.clienteEmail(), datos)));
            if (resultado.fallidos() == 0) {
                log.info("Evento enviado al stream: stream={} tipo={} eventoId={} cliente={}",
                        stream, evento.tipo(), evento.eventoId(), evento.clienteEmail());
            } else {
                log.error("No se pudo enviar al stream: stream={} tipo={} eventoId={}",
                        stream, evento.tipo(), evento.eventoId());
            }
        } catch (RuntimeException e) {
            log.error("Error al publicar en el stream: stream={} eventoId={} causa={}",
                    stream, evento.eventoId(), e.getMessage());
        }
    }
}
