package cl.duoc.streamingservice.evento;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;

/** Evento del stream actividad-clientes (mismo contrato que publica pedidos-service). */
public record ActividadClienteEvento(
        @NotBlank String eventoId,
        @NotBlank String tipo,
        @NotBlank @Email String clienteEmail,
        @NotBlank String producto,
        @NotBlank String ocurridoEn) {
}
