package cl.duoc.streamingservice.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Unica fuente de nombres y parametros de las aplicaciones consumidoras (application.yml).
 * Ninguna clase debe escribir a mano un nombre de stream, de aplicacion o de tabla.
 */
@Validated
@ConfigurationProperties(prefix = "streaming")
public record StreamingProperties(
        boolean habilitado,
        boolean simularFallos,
        @NotBlank String region,
        @NotNull @Valid Streams streams,
        @NotNull @Valid Consumidores consumidores,
        @NotNull @Valid Kcl kcl,
        @NotNull @Valid Procesamiento procesamiento,
        @NotNull @Valid Tablas tablas) {

    public record Streams(
            @NotNull @Valid Stream pedidos,
            @NotNull @Valid Stream actividad,
            @NotNull @Valid Stream fallidos) {
    }

    public record Stream(@NotBlank String nombre) {
    }

    public record Consumidores(
            @NotNull @Valid App notificaciones,
            @NotNull @Valid App analitica,
            @NotNull @Valid App tendencias) {
    }

    /** Una aplicacion consumidora: su nombre es tambien el de su tabla de coordinacion en DynamoDB. */
    public record App(boolean habilitada, @NotBlank String nombre, @NotBlank String stream) {
    }

    public record Kcl(
            @NotBlank String posicionInicial,
            @Min(200) long esperaEntreLecturasMs,
            @NotBlank String nivelMetricas,
            @Min(1) int apagadoGraciosoSegundos) {
    }

    public record Procesamiento(
            @Min(1) int maxIntentos,
            @Min(1) long esperaInicialMs,
            @Min(1) long esperaMaximaMs) {
    }

    public record Tablas(@NotBlank String contadores, @NotBlank String procesados) {
    }
}
