package cl.duoc.streamingservice.error;

/** Error que no se arregla reintentando (dato invalido): va directo al stream de fallidos. */
public class ErrorIrrecuperableException extends RuntimeException {

    public ErrorIrrecuperableException(String mensaje) {
        super(mensaje);
    }
}
