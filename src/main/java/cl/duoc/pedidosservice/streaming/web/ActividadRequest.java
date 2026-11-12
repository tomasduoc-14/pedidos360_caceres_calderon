package cl.duoc.pedidosservice.streaming.web;

import cl.duoc.pedidosservice.streaming.evento.TipoActividad;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record ActividadRequest(
        @NotNull TipoActividad tipo,
        @NotBlank @Email String clienteEmail,
        @NotBlank String producto) {
}
