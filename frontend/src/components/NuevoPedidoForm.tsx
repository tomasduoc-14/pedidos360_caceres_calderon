import { useState } from "react";
import type { NuevoPedidoInput } from "../types";

interface Props {
  onCrear: (input: NuevoPedidoInput) => Promise<void>;
}

export function NuevoPedidoForm({ onCrear }: Props) {
  const [producto, setProducto] = useState("");
  const [cantidad, setCantidad] = useState("1");
  const [enviando, setEnviando] = useState(false);
  const [errorLocal, setErrorLocal] = useState<string | null>(null);

  async function manejarEnvio(e: React.FormEvent) {
    e.preventDefault();
    setErrorLocal(null);

    const cantidadNumerica = Number(cantidad);
    if (!producto.trim()) {
      setErrorLocal("Indica qué producto incluye el pedido.");
      return;
    }
    if (!cantidadNumerica || cantidadNumerica <= 0) {
      setErrorLocal("Ingresa una cantidad válida, mayor a 0.");
      return;
    }

    setEnviando(true);
    try {
      await onCrear({ producto: producto.trim(), cantidad: cantidadNumerica });
      setProducto("");
      setCantidad("1");
    } finally {
      setEnviando(false);
    }
  }

  return (
    <form className="formulario-pedido" onSubmit={manejarEnvio}>
      <div className="campo">
        <label htmlFor="producto">Producto</label>
        <input
          id="producto"
          type="text"
          placeholder="Ej: Zapatillas RDS Test"
          value={producto}
          onChange={(e) => setProducto(e.target.value)}
          disabled={enviando}
        />
      </div>
      <div className="campo campo-total">
        <label htmlFor="cantidad">Cantidad</label>
        <input
          id="cantidad"
          type="number"
          min="1"
          step="1"
          placeholder="1"
          value={cantidad}
          onChange={(e) => setCantidad(e.target.value)}
          disabled={enviando}
        />
      </div>
      <button type="submit" className="boton-primario" disabled={enviando}>
        {enviando ? "Creando..." : "Crear pedido"}
      </button>
      {errorLocal && <p className="mensaje-error">{errorLocal}</p>}
    </form>
  );
}
