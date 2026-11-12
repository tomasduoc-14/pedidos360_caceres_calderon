package cl.duoc.pedidosservice.config.streaming;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Unica fuente de nombres y parametros del streaming (Kinesis).
 * Los valores viven en application.yml; ninguna clase de negocio debe escribirlos a mano.
 */
@Validated
@ConfigurationProperties(prefix = "streaming")
public record StreamingProperties(
        boolean habilitado,
        @NotBlank String region,
        @NotNull @Valid Cliente cliente,
        @NotNull @Valid Productor productor,
        @NotNull @Valid Streams streams) {

    /** Propiedades avanzadas del cliente de Kinesis (reintentos y timeouts). */
    public record Cliente(
            @NotBlank String retryMode,
            @Min(1) int apiCallTimeoutSegundos,
            @Min(1) int apiCallAttemptTimeoutSegundos) {
    }

    /** Reintento de los registros que Kinesis rechaza en un envio por lote (PutRecords). */
    public record Productor(
            @Min(1) @Max(500) int maxRegistrosPorLote,
            @Min(1) int maxIntentos,
            @Min(1) long esperaInicialMs,
            @Min(1) long esperaMaximaMs) {
    }

    public record Streams(
            @NotNull @Valid Stream pedidos,
            @NotNull @Valid Stream actividad,
            @NotNull @Valid Stream fallidos) {
    }

    public record Stream(
            @NotBlank String nombre,
            @Min(1) int shards,
            @Min(24) @Max(8760) int retencionHoras,
            @NotBlank String modoCapacidad) {
    }
}
