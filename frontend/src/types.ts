export type Rol = "ADMIN" | "CLIENTE";

export type EstadoPedido =
  | "PENDIENTE"
  | "ENVIADO"
  | "ENTREGADO";

export interface Pedido {
  id: number;
  producto: string;
  cantidad: number;
  clienteEmail: string;
  estado: EstadoPedido;
  fechaCreacion: string;
}

export interface NuevoPedidoInput {
  producto: string;
  cantidad: number;
}

export interface NuevoPedidoPayload extends NuevoPedidoInput {
  clienteEmail: string;
}
