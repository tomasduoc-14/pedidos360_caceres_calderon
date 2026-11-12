package cl.duoc.pedidosservice.streaming.productor;

import cl.duoc.pedidosservice.streaming.evento.EstadoPedidoEvento;

/** Unico punto por el que la logica de pedidos publica en el stream pedidos-estado. No expone el SDK. */
public interface PedidoStreamPublicador {

    void publicar(EstadoPedidoEvento evento);
}
