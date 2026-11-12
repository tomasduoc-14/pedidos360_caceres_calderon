package cl.duoc.pedidosservice.streaming.productor;

import cl.duoc.pedidosservice.config.streaming.StreamingProperties;
import cl.duoc.pedidosservice.streaming.evento.EstadoPedidoEvento;
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
public class KinesisPedidoStreamPublicador implements PedidoStreamPublicador {

    private static final Logger log = LoggerFactory.getLogger(KinesisPedidoStreamPublicador.class);

    private final KinesisProductor productor;
    private final JsonMapper json;
    private final StreamingProperties props;

    public KinesisPedidoStreamPublicador(KinesisProductor productor, JsonMapper json, StreamingProperties props) {
        this.productor = productor;
        this.json = json;
        this.props = props;
    }

    @Override
    public void publicar(EstadoPedidoEvento evento) {
        String stream = props.streams().pedidos().nombre();
        try {
            byte[] datos = json.writeValueAsString(evento).getBytes(StandardCharsets.UTF_8);
            // Clave de particion = pedidoId: el orden de los eventos de un pedido queda garantizado.
            String clave = String.valueOf(evento.pedidoId());
            ResultadoEnvio resultado = productor.enviar(stream, List.of(new Entrada(evento.eventoId(), clave, datos)));
            if (resultado.fallidos() == 0) {
                log.info("Evento enviado al stream: stream={} tipo={} eventoId={} pedidoId={} clave={}",
                        stream, evento.tipo(), evento.eventoId(), evento.pedidoId(), clave);
            } else {
                log.error("No se pudo enviar al stream: stream={} tipo={} eventoId={} pedidoId={}",
                        stream, evento.tipo(), evento.eventoId(), evento.pedidoId());
            }
        } catch (RuntimeException e) {
            // El streaming no debe romper la operacion de negocio del pedido.
            log.error("Error al publicar en el stream: stream={} eventoId={} causa={}",
                    stream, evento.eventoId(), e.getMessage());
        }
    }
}
