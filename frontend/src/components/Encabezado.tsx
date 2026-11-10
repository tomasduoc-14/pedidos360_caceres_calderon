import type { Rol } from "../types";

interface Props {
  correo: string | undefined;
  rol: Rol;
  onCerrarSesion: () => void;
}

export function Encabezado({ correo, rol, onCerrarSesion }: Props) {
  return (
    <header className="encabezado">
      <div className="marca">
        <span className="marca-nombre">Pedidos360</span>
      </div>
      <div className="usuario">
        <span className={`insignia-rol insignia-${rol.toLowerCase()}`}>{rol}</span>
        <span className="usuario-correo">{correo}</span>
        <button className="boton-secundario" onClick={onCerrarSesion}>
          Cerrar sesión
        </button>
      </div>
    </header>
  );
}
