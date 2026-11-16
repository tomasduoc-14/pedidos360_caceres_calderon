package cl.duoc.adminservice.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.util.List;

/** Cuerpos de entrada y salida de la API de administracion de streams (Kinesis). */
public final class StreamDtos {

    private StreamDtos() {
    }

    // ---------------------------------------------------------------- entrada

    /** Todos los campos menos "nombre" son opcionales: salen de kinesis-admin.por-defecto. */
    public record CrearStreamRequest(
            @NotBlank @Pattern(regexp = "[A-Za-z0-9_.-]{1,128}", message = "solo letras, numeros, - _ . (max 128)")
            String nombre,
            @Min(1) @Max(10000) Integer shards,
            @Min(24) @Max(8760) Integer retencionHoras,
            @Pattern(regexp = "PROVISIONED|ON_DEMAND", message = "debe ser PROVISIONED u ON_DEMAND")
            String modo) {
    }

    public record ActualizarShardsRequest(@NotNull @Min(1) @Max(10000) Integer shards) {
    }

    public record ActualizarRetencionRequest(@NotNull @Min(24) @Max(8760) Integer horas) {
    }

    // ----------------------------------------------------------------- salida

    public record StreamResumen(String nombre, String estado, String modo, int shards, int retencionHoras, String arn) {
    }

    public record ShardInfo(String shardId, String hashDesde, String hashHasta, String secuenciaInicial,
                            String padre, boolean abierto) {
    }

    public record StreamDetalle(StreamResumen resumen, List<ShardInfo> shards) {
    }

    public record StreamCreado(StreamResumen stream, List<String> advertencias) {
    }

    public record CambioShards(String stream, int shardsActuales, int shardsObjetivo, String nota) {
    }

    public record CambioRetencion(String stream, int horasAnteriores, int horasActuales) {
    }

    /** Un shard visto desde una aplicacion consumidora (fila de su tabla de coordinacion). */
    public record ShardConsumo(String shardId, String dueno, String checkpoint, Long subSecuencia,
                               Long contadorLease, Long lagMs, String estado) {
        public ShardConsumo conLag(Long nuevoLag) {
            return new ShardConsumo(shardId, dueno, checkpoint, subSecuencia, contadorLease, nuevoLag, estado);
        }
    }

    public record AplicacionResumen(String aplicacion, String stream, String tabla, int shardsAsignados,
                                    int shardsSinAsignar, List<String> instancias) {
    }

    public record AplicacionEstado(String aplicacion, String stream, String tabla, List<String> instancias,
                                   List<ShardConsumo> shards) {
    }

    public record PuntoLag(String hora, double maximoMs) {
    }

    public record LagStream(String stream, int ventanaMinutos, Double maximoMs, List<PuntoLag> puntos) {
    }
}
