package cl.duoc.streamingservice.dominio;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/** Activo cuando el streaming esta apagado (desarrollo y pruebas sin AWS). */
@Component
@ConditionalOnProperty(name = "streaming.habilitado", havingValue = "false", matchIfMissing = true)
public class ContadoresMemoria implements ContadoresRepositorio {

    private final Map<String, LongAdder> totales = new ConcurrentHashMap<>();

    @Override
    public void incrementar(String clave, long delta) {
        totales.computeIfAbsent(clave, k -> new LongAdder()).add(delta);
    }

    @Override
    public Map<String, Long> listar(String prefijo) {
        Map<String, Long> resultado = new TreeMap<>();
        totales.forEach((clave, total) -> {
            if (clave.startsWith(prefijo)) {
                resultado.put(clave.substring(prefijo.length()), total.sum());
            }
        });
        return resultado;
    }
}
