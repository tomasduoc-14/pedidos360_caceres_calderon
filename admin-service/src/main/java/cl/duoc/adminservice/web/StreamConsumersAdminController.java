package cl.duoc.adminservice.web;

import cl.duoc.adminservice.dto.StreamDtos.AplicacionEstado;
import cl.duoc.adminservice.dto.StreamDtos.AplicacionResumen;
import cl.duoc.adminservice.service.StreamAdminService;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Aplicaciones consumidoras (KCL): shards asignados, checkpoint y atraso. Solo delega. */
@RestController
@RequestMapping("/api/admin/stream-consumers")
public class StreamConsumersAdminController {

    private final StreamAdminService servicio;

    public StreamConsumersAdminController(StreamAdminService servicio) {
        this.servicio = servicio;
    }

    @GetMapping
    public List<AplicacionResumen> listar() {
        return servicio.listarAplicaciones();
    }

    @GetMapping("/{aplicacion}")
    public AplicacionEstado estado(@PathVariable String aplicacion) {
        return servicio.estadoAplicacion(aplicacion);
    }
}
