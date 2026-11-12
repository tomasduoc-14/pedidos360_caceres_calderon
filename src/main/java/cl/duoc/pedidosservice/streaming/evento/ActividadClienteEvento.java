package cl.duoc.pedidosservice.streaming.evento;

import java.time.Instant;
import java.util.UUID;

/**
 * Evento del stream actividad-clientes (vistas y clics). Clave de particion: clienteEmail
 * (la actividad de un cliente se lee en orden y los clientes se reparten entre los shards).
 */
public record ActividadClienteEvento(
        String eventoId,
        String tipo,
        String clienteEmail,
        String producto,
        String ocurridoEn) {

    public static ActividadClienteEvento de(TipoActividad tipo, String clienteEmail, String producto) {
        return new ActividadClienteEvento(UUID.randomUUID().toString(), tipo.name(),
                clienteEmail, producto, Instant.now().toString());
    }
}
