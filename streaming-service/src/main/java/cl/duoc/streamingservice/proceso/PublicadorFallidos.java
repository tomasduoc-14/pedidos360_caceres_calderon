package cl.duoc.streamingservice.proceso;

import cl.duoc.streamingservice.evento.EventoFallido;

/** Escribe en el stream de mensajes fallidos. Devuelve false si no pudo. */
public interface PublicadorFallidos {

    boolean enviar(EventoFallido evento);
}
