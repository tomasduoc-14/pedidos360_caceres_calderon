import { useEffect, useState } from "react";
import { fetchAuthSession } from "aws-amplify/auth";
import type { Rol } from "../types";

interface UseUserRoleResult {
  rol: Rol | null;
  loading: boolean;
  error: string | null;
}

/**
 * Obtiene la sesión de Cognito vigente y lee el grupo del usuario
 * (cognito:groups) desde los claims del ID token para saber si
 * debe ver la vista de ADMIN o la de CLIENTE.
 */
export function useUserRole(): UseUserRoleResult {
  const [rol, setRol] = useState<Rol | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelado = false;

    async function cargarRol() {
      try {
        const session = await fetchAuthSession();
        const grupos = session.tokens?.idToken?.payload?.[
          "cognito:groups"
        ] as string[] | undefined;

        if (cancelado) return;

        if (grupos?.includes("ADMIN")) {
          setRol("ADMIN");
        } else if (grupos?.includes("CLIENTE")) {
          setRol("CLIENTE");
        } else {
          setError("El usuario no pertenece a ningún grupo conocido (ADMIN/CLIENTE).");
        }
      } catch (err) {
        if (!cancelado) {
          setError(err instanceof Error ? err.message : "No se pudo leer la sesión.");
        }
      } finally {
        if (!cancelado) setLoading(false);
      }
    }

    cargarRol();
    return () => {
      cancelado = true;
    };
  }, []);

  return { rol, loading, error };
}
