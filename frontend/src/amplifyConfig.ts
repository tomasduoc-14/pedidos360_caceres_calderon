import { Amplify } from "aws-amplify";

// Todos los valores se leen desde variables de entorno (.env).
// Ver .env.example para la lista de variables requeridas.
const userPoolId = import.meta.env.VITE_COGNITO_USER_POOL_ID as string;
const userPoolClientId = import.meta.env.VITE_COGNITO_CLIENT_ID as string;

if (!userPoolId || !userPoolClientId) {
  // Falla temprano y con un mensaje claro si falta configuración,
  // en vez de dejar que Amplify falle más adelante de forma críptica.
  throw new Error(
    "Faltan variables de entorno de Cognito. Revisa tu archivo .env " +
      "(VITE_COGNITO_USER_POOL_ID y VITE_COGNITO_CLIENT_ID)."
  );
}

Amplify.configure({
  Auth: {
    Cognito: {
      userPoolId,
      userPoolClientId,
    },
  },
});
