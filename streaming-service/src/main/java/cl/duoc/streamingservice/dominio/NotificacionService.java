package cl.duoc.streamingservice.dominio;

import cl.duoc.streamingservice.config.StreamingProperties;
import cl.duoc.streamingservice.error.ErrorIrrecuperableException;
import cl.duoc.streamingservice.error.ErrorTransitorioException;
import cl.duoc.streamingservice.evento.EstadoPedidoEvento;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Aplicacion consumidora "notificaciones-app": avisa al cliente cada vez que cambia el estado de su pedido.
 * Con streaming.simular-fallos=true, ciertos productos fuerzan errores para la demostracion:
 * FALLO_TRANSITORIO (falla 2 veces y luego funciona), FALLO_PERSISTENTE (siempre falla, transitorio)
 * y FALLO_IRRECUPERABLE (dato invalido).
 */
@Service
public class NotificacionService implements ProcesadorEvento<EstadoPedidoEvento> {

    private static final Logger log = LoggerFactory.getLogger(NotificacionService.class);

    private final boolean simularFallos;
    private final Map<String, Integer> intentosPorEvento = new ConcurrentHashMap<>();

    public NotificacionService(StreamingProperties props) {
        this.simularFallos = props.simularFallos();
    }

    @Override
    public void procesar(EstadoPedidoEvento evento) {
        if (simularFallos) {
            simular(evento);
        }
        log.info("NOTIFICACION a {}: tu pedido #{} ({} x{}) cambio a {}",
                evento.clienteEmail(), evento.pedidoId(), evento.producto(), evento.cantidad(), evento.estado());
    }

    private void simular(EstadoPedidoEvento evento) {
        switch (evento.producto()) {
            case "FALLO_TRANSITORIO" -> {
                int vez = intentosPorEvento.merge(evento.eventoId(), 1, Integer::sum);
                if (vez <= 2) {
                    throw new ErrorTransitorioException("servicio de correo no disponible (simulado)");
                }
                intentosPorEvento.remove(evento.eventoId());
            }
            case "FALLO_PERSISTENTE" ->
                    throw new ErrorTransitorioException("servicio de correo caido (simulado)");
            case "FALLO_IRRECUPERABLE" ->
                    throw new ErrorIrrecuperableException("destinatario invalido (simulado)");
            default -> {
            }
        }
    }
}
