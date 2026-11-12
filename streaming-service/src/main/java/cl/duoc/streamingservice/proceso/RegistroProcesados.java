package cl.duoc.streamingservice.proceso;

/** Memoria de los eventoId ya procesados por cada aplicacion (tolerancia a duplicados). */
public interface RegistroProcesados {

    boolean yaProcesado(String aplicacion, String eventoId);

    void marcar(String aplicacion, String eventoId);
}
