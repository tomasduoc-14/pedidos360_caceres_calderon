package cl.duoc.streamingservice.proceso;

public enum ResultadoProceso {
    /** Se proceso bien. */
    PROCESADO,
    /** Ya se habia procesado antes (mismo eventoId): se ignora. */
    DUPLICADO,
    /** Invalido, irrecuperable o reintentos agotados: se escribio en el stream de fallidos. */
    ENVIADO_A_FALLIDOS,
    /** Ni se pudo procesar ni enviar a fallidos: queda solo en el log (ultimo recurso). */
    PERDIDO
}
