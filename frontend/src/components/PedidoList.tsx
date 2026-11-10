import type { EstadoPedido, Pedido, Rol } from "../types";

const ESTADOS: EstadoPedido[] = [
  "PENDIENTE",
  "ENVIADO",
  "ENTREGADO",
];

const ETIQUETA_ESTADO: Record<EstadoPedido, string> = {
  PENDIENTE: "Pendiente",
  ENVIADO: "Enviado",
  ENTREGADO: "Entregado",
};

interface Props {
  pedidos: Pedido[];
  rol: Rol;
  onCambiarEstado: (id: number, estado: EstadoPedido) => void;
  onEliminar: (id: number) => void;
}

export function PedidoList({ pedidos, rol, onCambiarEstado, onEliminar }: Props) {
  if (pedidos.length === 0) {
    return (
      <div className="estado-vacio">
        <p>Todavía no hay pedidos.</p>
        {rol === "CLIENTE" && <p>Crea el primero con el formulario de arriba.</p>}
      </div>
    );
  }

  return (
    <table className="tabla-pedidos">
      <thead>
        <tr>
          <th>ID</th>
          <th>Producto</th>
          <th>Cantidad</th>
          <th>Cliente</th>
          <th>Estado</th>
          <th>Creado</th>
          {rol === "ADMIN" && <th>Acciones</th>}
        </tr>
      </thead>
      <tbody>
        {pedidos.map((pedido) => (
          <tr key={pedido.id}>
            <td className="celda-id">#{pedido.id}</td>
            <td>{pedido.producto}</td>
            <td className="celda-total">{pedido.cantidad}</td>
            <td>{pedido.clienteEmail}</td>
            <td>
              <span className={`pastilla-estado estado-${pedido.estado.toLowerCase()}`}>
                {ETIQUETA_ESTADO[pedido.estado]}
              </span>
            </td>
            <td className="celda-fecha">
              {new Date(pedido.fechaCreacion).toLocaleDateString("es-CL")}
            </td>
            {rol === "ADMIN" && (
              <td className="celda-acciones">
                <select
                  value={pedido.estado}
                  onChange={(e) =>
                    onCambiarEstado(pedido.id, e.target.value as EstadoPedido)
                  }
                >
                  {ESTADOS.map((estado) => (
                    <option key={estado} value={estado}>
                      {ETIQUETA_ESTADO[estado]}
                    </option>
                  ))}
                </select>
                <button
                  className="boton-peligro"
                  onClick={() => onEliminar(pedido.id)}
                  aria-label={`Eliminar pedido #${pedido.id}`}
                >
                  Eliminar
                </button>
              </td>
            )}
          </tr>
        ))}
      </tbody>
    </table>
  );
}
