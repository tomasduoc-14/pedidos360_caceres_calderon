package cl.duoc.streamingservice.dominio;

/** Logica de negocio de un evento. No sabe nada de Kinesis. Lanza los errores de cl.duoc.streamingservice.error. */
@FunctionalInterface
public interface ProcesadorEvento<T> {

    void procesar(T evento);
}
