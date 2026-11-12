package cl.duoc.pedidosservice.streaming.web;

import cl.duoc.pedidosservice.service.ActividadService;
import jakarta.validation.Valid;
import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Recibe vistas y clics del frontend y los publica en el stream actividad-clientes. */
@RestController
@RequestMapping("/api/actividad")
public class ActividadController {

    private final ActividadService servicio;

    public ActividadController(ActividadService servicio) {
        this.servicio = servicio;
    }

    @PostMapping
    public ResponseEntity<Map<String, String>> registrar(@Valid @RequestBody ActividadRequest req) {
        String eventoId = servicio.registrar(req.tipo(), req.clienteEmail(), req.producto());
        // 202: el evento se acepto para procesarse; no hay resultado inmediato que devolver.
        return ResponseEntity.accepted().body(Map.of("eventoId", eventoId));
    }
}
