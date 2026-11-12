package cl.duoc.pedidosservice.streaming.evento;

import cl.duoc.pedidosservice.mensajeria.evento.TipoEvento;
import java.time.Instant;
import java.util.UUID;

/**
 * Evento del stream pedidos-estado. Clave de particion: pedidoId (todos los eventos de un
 * pedido caen en el mismo shard y se leen en orden). eventoId permite detectar duplicados.
 */
public record EstadoPedidoEvento(
        String eventoId,
        String tipo,
        Long pedidoId,
        String producto,
        Integer cantidad,
        String clienteEmail,
        String estado,
        String ocurridoEn) {

    public static EstadoPedidoEvento de(TipoEvento tipo, Long pedidoId, String producto,
                                        Integer cantidad, String clienteEmail, String estado) {
        return new EstadoPedidoEvento(UUID.randomUUID().toString(), tipo.name(), pedidoId,
                producto, cantidad, clienteEmail, estado, Instant.now().toString());
    }
}
