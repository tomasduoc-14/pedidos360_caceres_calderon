package cl.duoc.streamingservice.evento;

/**
 * Lo que se escribe en el stream de mensajes fallidos: el evento original tal como llego (payload)
 * y todo lo necesario para investigar y relacionarlo con los logs.
 */
public record EventoFallido(
        String eventoId,
        String aplicacion,
        String streamOrigen,
        String shardId,
        String sequenceNumber,
        String causa,
        int intentos,
        String payload,
        String fallidoEn) {
}
