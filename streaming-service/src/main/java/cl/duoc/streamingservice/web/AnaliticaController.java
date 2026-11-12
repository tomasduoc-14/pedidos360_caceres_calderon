package cl.duoc.streamingservice.web;

import cl.duoc.streamingservice.dominio.AnaliticaPedidosService;
import cl.duoc.streamingservice.dominio.TendenciasService;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Estado derivado de los streams, casi en tiempo real. El frontend lo consulta por el API Gateway. */
@RestController
@RequestMapping("/api/analitica")
public class AnaliticaController {

    private final AnaliticaPedidosService analitica;
    private final TendenciasService tendencias;

    public AnaliticaController(AnaliticaPedidosService analitica, TendenciasService tendencias) {
        this.analitica = analitica;
        this.tendencias = tendencias;
    }

    @GetMapping("/pedidos")
    public Map<String, Map<String, Long>> pedidos() {
        return analitica.resumen();
    }

    @GetMapping("/tendencias")
    public Map<String, Map<String, Long>> tendencias() {
        return tendencias.resumen();
    }
}
