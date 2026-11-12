package cl.duoc.pedidosservice.service;

import cl.duoc.pedidosservice.mensajeria.evento.PedidoEvento;
import cl.duoc.pedidosservice.mensajeria.evento.TipoEvento;
import cl.duoc.pedidosservice.mensajeria.publicador.PedidoEventoPublicador;
import cl.duoc.pedidosservice.model.EstadoPedido;
import cl.duoc.pedidosservice.model.Pedido;
import cl.duoc.pedidosservice.repository.PedidoRepository;
import cl.duoc.pedidosservice.streaming.evento.EstadoPedidoEvento;
import cl.duoc.pedidosservice.streaming.productor.PedidoStreamPublicador;
import java.util.List;
import org.springframework.stereotype.Service;

@Service
public class PedidoService {

    private final PedidoRepository pedidoRepository;

    /** Parcial 2: notificaciones, tickets y auditoria por SNS/SQS. */
    private final PedidoEventoPublicador publicador;

    /** Parcial 3: el mismo hecho de negocio, como evento de streaming en Kinesis. */
    private final PedidoStreamPublicador streamPublicador;

    public PedidoService(PedidoRepository pedidoRepository, PedidoEventoPublicador publicador,
                         PedidoStreamPublicador streamPublicador) {
        this.pedidoRepository = pedidoRepository;
        this.publicador = publicador;
        this.streamPublicador = streamPublicador;
    }

    private void publicar(TipoEvento tipo, Pedido p) {
        String estado = p.getEstado() == null ? null : p.getEstado().name();
        publicador.publicar(PedidoEvento.de(tipo, p.getId(), p.getProducto(), p.getCantidad(),
                p.getClienteEmail(), estado));
        streamPublicador.publicar(EstadoPedidoEvento.de(tipo, p.getId(), p.getProducto(), p.getCantidad(),
                p.getClienteEmail(), estado));
    }

    public List<Pedido> listarTodos() {
        return pedidoRepository.findAll();
    }

    public Pedido buscarPorId(Long id) {
        return pedidoRepository.findById(id)
                .orElseThrow(() -> new PedidoNoEncontradoException(id));
    }

    public Pedido crear(Pedido nuevo) {
        Pedido guardado = pedidoRepository.save(nuevo);
        publicar(TipoEvento.PEDIDO_CREADO, guardado);
        return guardado;
    }

    public Pedido actualizarEstado(Long id, EstadoPedido nuevoEstado) {
        Pedido pedido = buscarPorId(id);
        pedido.setEstado(nuevoEstado);
        Pedido actualizado = pedidoRepository.save(pedido);
        if (nuevoEstado == EstadoPedido.ENVIADO) {
            publicar(TipoEvento.PEDIDO_ENVIADO, actualizado);
        } else if (nuevoEstado == EstadoPedido.ENTREGADO) {
            publicar(TipoEvento.PEDIDO_ENTREGADO, actualizado);
        }
        return actualizado;
    }

    public void eliminar(Long id) {
        if (!pedidoRepository.existsById(id)) {
            throw new PedidoNoEncontradoException(id);
        }
        pedidoRepository.deleteById(id);
    }
}
