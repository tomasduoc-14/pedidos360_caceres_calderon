package cl.duoc.streamingservice.evento;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/** Evento del stream pedidos-estado (mismo contrato que publica pedidos-service). */
public record EstadoPedidoEvento(
        @NotBlank String eventoId,
        @NotBlank String tipo,
        @NotNull Long pedidoId,
        @NotBlank String producto,
        @NotNull @Min(1) Integer cantidad,
        @NotBlank @Email String clienteEmail,
        @NotBlank String estado,
        @NotBlank String ocurridoEn) {
}
