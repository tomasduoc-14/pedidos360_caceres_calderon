package cl.duoc.pedidosservice.streaming.productor;

import cl.duoc.pedidosservice.streaming.evento.EstadoPedidoEvento;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/** Activo cuando el streaming esta apagado (desarrollo local sin AWS). */
@Component
@ConditionalOnProperty(name = "streaming.habilitado", havingValue = "false", matchIfMissing = true)
public class NoopPedidoStreamPublicador implements PedidoStreamPublicador {

    private static final Logger log = LoggerFactory.getLogger(NoopPedidoStreamPublicador.class);

    @Override
    public void publicar(EstadoPedidoEvento evento) {
        log.debug("Streaming deshabilitado, evento descartado: {}", evento.eventoId());
    }
}
