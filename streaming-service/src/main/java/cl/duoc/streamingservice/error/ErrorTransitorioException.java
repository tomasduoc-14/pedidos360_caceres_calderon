package cl.duoc.streamingservice.error;

/** Error que puede desaparecer solo (servicio externo caido, timeout): se reintenta. */
public class ErrorTransitorioException extends RuntimeException {

    public ErrorTransitorioException(String mensaje) {
        super(mensaje);
    }
}
