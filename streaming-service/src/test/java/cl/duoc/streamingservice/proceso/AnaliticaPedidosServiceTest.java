package cl.duoc.streamingservice.proceso;

import static org.assertj.core.api.Assertions.assertThat;

import cl.duoc.streamingservice.dominio.AnaliticaPedidosService;
import cl.duoc.streamingservice.dominio.ContadoresMemoria;
import cl.duoc.streamingservice.evento.EstadoPedidoEvento;
import org.junit.jupiter.api.Test;

class AnaliticaPedidosServiceTest {

    private static EstadoPedidoEvento evento(String tipo, String producto, int cantidad) {
        return new EstadoPedidoEvento("id-" + tipo + producto, tipo, 1L, producto, cantidad,
                "cliente@duoc.cl", "PENDIENTE", "2026-11-12T12:00:00Z");
    }

    @Test
    void cuentaEventosPorTipoYUnidadesPorProducto() {
        AnaliticaPedidosService servicio = new AnaliticaPedidosService(new ContadoresMemoria());

        servicio.procesar(evento("PEDIDO_CREADO", "Parlante", 2));
        servicio.procesar(evento("PEDIDO_CREADO", "Parlante", 3));
        servicio.procesar(evento("PEDIDO_ENVIADO", "Parlante", 5));

        var resumen = servicio.resumen();
        assertThat(resumen.get("eventosPorTipo")).containsEntry("PEDIDO_CREADO", 2L).containsEntry("PEDIDO_ENVIADO", 1L);
        assertThat(resumen.get("unidadesPorProducto")).containsEntry("Parlante", 5L);
    }
}
