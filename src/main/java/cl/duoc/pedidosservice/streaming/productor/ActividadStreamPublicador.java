package cl.duoc.pedidosservice.streaming.productor;

import cl.duoc.pedidosservice.streaming.evento.ActividadClienteEvento;

/** Unico punto por el que la logica de negocio publica en el stream actividad-clientes. */
public interface ActividadStreamPublicador {

    void publicar(ActividadClienteEvento evento);
}
