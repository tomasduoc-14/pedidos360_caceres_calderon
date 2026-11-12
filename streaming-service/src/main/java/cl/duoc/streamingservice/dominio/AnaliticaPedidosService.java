package cl.duoc.streamingservice.dominio;

import cl.duoc.streamingservice.evento.EstadoPedidoEvento;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.stereotype.Service;

/**
 * Aplicacion consumidora "analitica-app": totales en linea de los pedidos.
 * Lee el mismo stream que notificaciones-app, pero con su propio avance (checkpoint).
 */
@Service
public class AnaliticaPedidosService implements ProcesadorEvento<EstadoPedidoEvento> {

    static final String PREFIJO_TIPO = "tipo#";
    static final String PREFIJO_PRODUCTO = "producto#";
    private static final String TIPO_CREADO = "PEDIDO_CREADO";

    private final ContadoresRepositorio contadores;

    public AnaliticaPedidosService(ContadoresRepositorio contadores) {
        this.contadores = contadores;
    }

    @Override
    public void procesar(EstadoPedidoEvento evento) {
        contadores.incrementar(PREFIJO_TIPO + evento.tipo(), 1);
        if (TIPO_CREADO.equals(evento.tipo())) {
            contadores.incrementar(PREFIJO_PRODUCTO + evento.producto(), evento.cantidad());
        }
    }

    public Map<String, Map<String, Long>> resumen() {
        Map<String, Map<String, Long>> resumen = new LinkedHashMap<>();
        resumen.put("eventosPorTipo", contadores.listar(PREFIJO_TIPO));
        resumen.put("unidadesPorProducto", contadores.listar(PREFIJO_PRODUCTO));
        return resumen;
    }
}
