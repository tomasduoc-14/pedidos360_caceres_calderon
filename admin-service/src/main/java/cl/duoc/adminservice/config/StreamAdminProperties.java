package cl.duoc.adminservice.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Configuracion del administrador de streams (application.yml, prefijo "kinesis-admin").
 * Los nombres de las aplicaciones consumidoras deben coincidir con los de streaming-service:
 * cada nombre es tambien el de su tabla de coordinacion en DynamoDB.
 */
@Validated
@ConfigurationProperties(prefix = "kinesis-admin")
public record StreamAdminProperties(
        @NotBlank String region,
        @NotNull @Valid PorDefecto porDefecto,
        @NotNull @Valid List<Aplicacion> aplicaciones,
        @Min(1) int ventanaLagMinutos) {

    public record PorDefecto(@Min(1) int shards, @Min(24) int retencionHoras) {
    }

    public record Aplicacion(@NotBlank String nombre, @NotBlank String stream) {
    }
}
