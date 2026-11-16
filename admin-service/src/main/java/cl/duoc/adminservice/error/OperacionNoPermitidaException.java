package cl.duoc.adminservice.error;

/** La operacion existe pero el entorno (por ejemplo una politica del Learner Lab) no la permite. */
public class OperacionNoPermitidaException extends RuntimeException {
    public OperacionNoPermitidaException(String mensaje) {
        super(mensaje);
    }
}
