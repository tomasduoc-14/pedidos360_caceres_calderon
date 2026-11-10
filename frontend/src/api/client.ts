import { fetchAuthSession } from "aws-amplify/auth";
import type { EstadoPedido, NuevoPedidoPayload, Pedido } from "../types";

const API_URL = import.meta.env.VITE_API_URL as string;

export class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
    this.name = "ApiError";
  }
}

/**
 * Interceptor central de peticiones: agrega siempre el header
 * Authorization: Bearer <accessToken> obtenido de la sesión de Cognito.
 */
async function apiFetch(path: string, options: RequestInit = {}): Promise<Response> {
  const session = await fetchAuthSession();
  const accessToken = session.tokens?.accessToken?.toString();

  if (!accessToken) {
    throw new ApiError(401, "No hay sesión activa. Vuelve a iniciar sesión.");
  }

  const response = await fetch(`${API_URL}${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${accessToken}`,
      ...options.headers,
    },
  });

  if (!response.ok) {
    let mensaje = `Error ${response.status} al comunicarse con la API`;
    try {
      const cuerpo = await response.json();
      if (cuerpo?.message) mensaje = cuerpo.message;
    } catch {
      // el cuerpo no era JSON, se mantiene el mensaje genérico
    }
    throw new ApiError(response.status, mensaje);
  }

  return response;
}

export async function getPedidos(): Promise<Pedido[]> {
  const res = await apiFetch("/api/pedidos");
  return res.json();
}

export async function crearPedido(input: NuevoPedidoPayload): Promise<Pedido> {
  const res = await apiFetch("/api/pedidos", {
    method: "POST",
    body: JSON.stringify(input),
  });
  return res.json();
}

export async function actualizarEstadoPedido(
  id: number,
  estado: EstadoPedido
): Promise<Pedido> {
  const res = await apiFetch(`/api/pedidos/${id}/estado`, {
    method: "PUT",
    body: JSON.stringify({ estado }),
  });
  return res.json();
}

export async function eliminarPedido(id: number): Promise<void> {
  await apiFetch(`/api/pedidos/${id}`, { method: "DELETE" });
}
