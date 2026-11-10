import { useEffect, useState } from "react";
import { useAuthenticator } from "@aws-amplify/ui-react";
import { useUserRole } from "../hooks/useUserRole";
import {
  actualizarEstadoPedido,
  crearPedido,
  eliminarPedido,
  getPedidos,
} from "../api/client";
import type { EstadoPedido, NuevoPedidoInput, Pedido } from "../types";
import { Encabezado } from "../components/Encabezado";
import { NuevoPedidoForm } from "../components/NuevoPedidoForm";
import { PedidoList } from "../components/PedidoList";

export function Dashboard() {
  const { user, signOut } = useAuthenticator((ctx) => [ctx.user]);
  const { rol, loading: cargandoRol, error: errorRol } = useUserRole();

  const [pedidos, setPedidos] = useState<Pedido[]>([]);
  const [cargandoPedidos, setCargandoPedidos] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!rol) return;
    cargarPedidos();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [rol]);

  async function cargarPedidos() {
    setCargandoPedidos(true);
    setError(null);
    try {
      const datos = await getPedidos();
      setPedidos(datos);
    } catch (err) {
      setError(err instanceof Error ? err.message : "No se pudieron cargar los pedidos.");
    } finally {
      setCargandoPedidos(false);
    }
  }

  async function manejarCrear(input: NuevoPedidoInput) {
    setError(null);
    const clienteEmail = user?.signInDetails?.loginId;
    if (!clienteEmail) {
      setError("No se pudo determinar tu correo de usuario. Vuelve a iniciar sesión.");
      return;
    }
    try {
      const nuevo = await crearPedido({ ...input, clienteEmail });
      setPedidos((prev) => [nuevo, ...prev]);
    } catch (err) {
      setError(err instanceof Error ? err.message : "No se pudo crear el pedido.");
    }
  }

  async function manejarCambioEstado(id: number, estado: EstadoPedido) {
    setError(null);
    const anteriores = pedidos;
    setPedidos((prev) => prev.map((p) => (p.id === id ? { ...p, estado } : p)));
    try {
      await actualizarEstadoPedido(id, estado);
    } catch (err) {
      setPedidos(anteriores);
      setError(err instanceof Error ? err.message : "No se pudo actualizar el estado.");
    }
  }

  async function manejarEliminar(id: number) {
    setError(null);
    const anteriores = pedidos;
    setPedidos((prev) => prev.filter((p) => p.id !== id));
    try {
      await eliminarPedido(id);
    } catch (err) {
      setPedidos(anteriores);
      setError(err instanceof Error ? err.message : "No se pudo eliminar el pedido.");
    }
  }

  if (cargandoRol) {
    return <div className="pantalla-carga">Verificando tu sesión...</div>;
  }

  if (errorRol || !rol) {
    return (
      <div className="pantalla-error">
        <p>{errorRol ?? "No se pudo determinar tu rol."}</p>
        <button className="boton-secundario" onClick={signOut}>
          Cerrar sesión
        </button>
      </div>
    );
  }

  return (
    <div className="panel">
      <Encabezado correo={user?.signInDetails?.loginId} rol={rol} onCerrarSesion={signOut} />

      <main className="contenido">
        <section className="seccion-nuevo-pedido">
          <h2>Nuevo pedido</h2>
          <NuevoPedidoForm onCrear={manejarCrear} />
        </section>

        <section className="seccion-lista">
          <div className="seccion-lista-titulo">
            <h2>{rol === "ADMIN" ? "Todos los pedidos" : "Mis pedidos"}</h2>
            <button className="boton-secundario" onClick={cargarPedidos} disabled={cargandoPedidos}>
              {cargandoPedidos ? "Actualizando..." : "Actualizar"}
            </button>
          </div>

          {error && <p className="mensaje-error">{error}</p>}

          {cargandoPedidos ? (
            <p className="pantalla-carga">Cargando pedidos...</p>
          ) : (
            <PedidoList
              pedidos={pedidos}
              rol={rol}
              onCambiarEstado={manejarCambioEstado}
              onEliminar={manejarEliminar}
            />
          )}
        </section>
      </main>
    </div>
  );
}
