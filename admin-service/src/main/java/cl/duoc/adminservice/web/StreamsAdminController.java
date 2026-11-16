package cl.duoc.adminservice.web;

import cl.duoc.adminservice.dto.StreamDtos.ActualizarRetencionRequest;
import cl.duoc.adminservice.dto.StreamDtos.ActualizarShardsRequest;
import cl.duoc.adminservice.dto.StreamDtos.CambioRetencion;
import cl.duoc.adminservice.dto.StreamDtos.CambioShards;
import cl.duoc.adminservice.dto.StreamDtos.CrearStreamRequest;
import cl.duoc.adminservice.dto.StreamDtos.LagStream;
import cl.duoc.adminservice.dto.StreamDtos.StreamCreado;
import cl.duoc.adminservice.dto.StreamDtos.StreamDetalle;
import cl.duoc.adminservice.dto.StreamDtos.StreamResumen;
import cl.duoc.adminservice.service.StreamAdminService;
import jakarta.validation.Valid;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Streams de Kinesis. Sin logica: delega todo en StreamAdminService. */
@RestController
@RequestMapping("/api/admin/streams")
public class StreamsAdminController {

    private final StreamAdminService servicio;

    public StreamsAdminController(StreamAdminService servicio) {
        this.servicio = servicio;
    }

    @GetMapping
    public List<StreamResumen> listar() {
        return servicio.listarStreams();
    }

    @PostMapping
    public ResponseEntity<StreamCreado> crear(@Valid @RequestBody CrearStreamRequest req) {
        return ResponseEntity.status(HttpStatus.CREATED).body(servicio.crearStream(req));
    }

    @GetMapping("/{nombre}")
    public StreamDetalle describir(@PathVariable String nombre) {
        return servicio.describirStream(nombre);
    }

    @PutMapping("/{nombre}/shards")
    public CambioShards actualizarShards(@PathVariable String nombre,
                                         @Valid @RequestBody ActualizarShardsRequest req) {
        return servicio.actualizarShards(nombre, req.shards());
    }

    @PutMapping("/{nombre}/retention")
    public CambioRetencion actualizarRetencion(@PathVariable String nombre,
                                               @Valid @RequestBody ActualizarRetencionRequest req) {
        return servicio.actualizarRetencion(nombre, req.horas());
    }

    @GetMapping("/{nombre}/lag")
    public LagStream lag(@PathVariable String nombre) {
        return servicio.lagDelStream(nombre);
    }

    @DeleteMapping("/{nombre}")
    public ResponseEntity<Void> eliminar(@PathVariable String nombre) {
        servicio.eliminarStream(nombre);
        return ResponseEntity.noContent().build();
    }
}
