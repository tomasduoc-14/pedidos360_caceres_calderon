package cl.duoc.streamingservice.dominio;

import cl.duoc.streamingservice.evento.ActividadClienteEvento;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.stereotype.Service;

/** Aplicacion consumidora "tendencias-app": vistas y agregados al carro por producto, en linea. */
@Service
public class TendenciasService implements ProcesadorEvento<ActividadClienteEvento> {

    static final String PREFIJO_VISTAS = "VISTA_PRODUCTO#";
    static final String PREFIJO_CARRO = "AGREGO_AL_CARRO#";

    private final ContadoresRepositorio contadores;

    public TendenciasService(ContadoresRepositorio contadores) {
        this.contadores = contadores;
    }

    @Override
    public void procesar(ActividadClienteEvento evento) {
        contadores.incrementar(evento.tipo() + "#" + evento.producto(), 1);
    }

    public Map<String, Map<String, Long>> resumen() {
        Map<String, Map<String, Long>> resumen = new LinkedHashMap<>();
        resumen.put("vistasPorProducto", contadores.listar(PREFIJO_VISTAS));
        resumen.put("agregadosAlCarroPorProducto", contadores.listar(PREFIJO_CARRO));
        return resumen;
    }
}
