package cl.duoc.streamingservice.dominio;

import java.util.Map;

/** Totales en linea. La implementacion real usa DynamoDB; sin AWS se usa una en memoria. */
public interface ContadoresRepositorio {

    void incrementar(String clave, long delta);

    /** Totales de las claves que empiezan con el prefijo (la clave se devuelve sin el prefijo). */
    Map<String, Long> listar(String prefijo);
}
