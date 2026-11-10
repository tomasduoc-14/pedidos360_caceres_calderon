# Pedidos360 — Frontend

Frontend en React + TypeScript (Vite) para el sistema Pedidos360. Usa **AWS Amplify**
con el componente `<Authenticator>` de `@aws-amplify/ui-react` para el login contra
**Amazon Cognito**, y consume el backend (Spring Boot en EC2, expuesto vía
**API Gateway**) adjuntando el JWT en cada llamada.

## 1. Requisitos

- Node.js 18+
- El backend de Pedidos360 ya desplegado (API Gateway) y el User Pool de Cognito
  con los grupos `ADMIN` y `CLIENTE` configurados.

## 2. Instalación

```bash
npm install
```

## 3. Variables de entorno

Copia `.env.example` a `.env` y completa con los datos de tu entorno
(el archivo `.env` de este proyecto ya viene completado con los valores
entregados para esta evaluación):

```bash
VITE_COGNITO_USER_POOL_ID=us-east-1_UyJcppibf
VITE_COGNITO_CLIENT_ID=2uudamt3adb8a2fpcau89um4v6
VITE_API_URL=https://badk2alir5.execute-api.us-east-1.amazonaws.com
```

`.env` está en `.gitignore`: no debe subirse al repositorio. Sube solo
`.env.example`, sin valores.

## 4. Ejecutar en desarrollo

```bash
npm run dev
```

## 5. Usuarios de prueba

| Usuario | Contraseña | Rol |
| --- | --- | --- |
| admin.prueba@duoc.cl | DuocNueva2026! | ADMIN |
| cliente.prueba@duoc.cl | DuocCliente2026! | CLIENTE |

El rol se lee del claim `cognito:groups` del ID token (ver `src/hooks/useUserRole.ts`).
Según el rol, el dashboard muestra u oculta las acciones de administración
(cambiar estado, eliminar).

## 6. Cómo está resuelto cada requisito de la pauta

- **Amplify + Authenticator**: `src/amplifyConfig.ts` configura `Amplify.configure`
  con el User Pool; `src/App.tsx` envuelve toda la app en `<Authenticator>` (login,
  logout y rutas protegidas quedan resueltos por el propio componente: mientras no
  hay sesión, `Authenticator` muestra el formulario de login en vez del `children`).
- **Lectura de roles/scopes**: `src/hooks/useUserRole.ts` usa `fetchAuthSession()`
  y lee `cognito:groups` del ID token.
- **Interceptor de peticiones (apiFetch)**: `src/api/client.ts` centraliza el fetch,
  obtiene el `accessToken` de la sesión vigente y agrega
  `Authorization: Bearer <accessToken>` a cada llamada.
- **Consumo del API Gateway**: `getPedidos`, `crearPedido`, `actualizarEstadoPedido`
  y `eliminarPedido` en `src/api/client.ts`, todas apuntando a
  `${VITE_API_URL}/api/pedidos...`.
- **Vistas por rol**: `src/pages/Dashboard.tsx` decide qué mostrar según `rol`;
  `src/components/PedidoList.tsx` solo renderiza la columna de acciones
  (cambiar estado / eliminar) cuando `rol === "ADMIN"`.

## 7. Endpoints consumidos

| Método | Ruta | Roles |
| --- | --- | --- |
| GET | `/api/pedidos` | ADMIN, CLIENTE |
| POST | `/api/pedidos` | ADMIN, CLIENTE |
| PUT | `/api/pedidos/{id}/estado` | ADMIN |
| DELETE | `/api/pedidos/{id}` | ADMIN |

## 8. Estructura

```
src/
  amplifyConfig.ts      # Amplify.configure con datos de Cognito
  api/client.ts          # apiFetch + funciones de la API de pedidos
  hooks/useUserRole.ts   # Lee cognito:groups del token
  components/            # Encabezado, formulario, tabla de pedidos
  pages/Dashboard.tsx    # Orquesta carga de datos y acciones por rol
  types.ts               # Tipos de dominio (Pedido, EstadoPedido, Rol)
```

## 9. Build de producción

```bash
npm run build
```

Genera `dist/`, listo para desplegar en el hosting estático que corresponda
(S3 + CloudFront, Amplify Hosting, etc.).
