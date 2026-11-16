# admin-service

Microservicio administrador de Pedidos360: mensajeria (SNS + SQS, Parcial 2) y streams (Kinesis, Parcial 3).
Puerto `8090`. La logica de colas esta en `ColaAdminService` y la de streams en `StreamAdminService`; los controladores solo delegan.

## Levantar
```powershell
# credenciales del Learner Lab y la clave del administrador (NO se suben al repo)
$env:AWS_ACCESS_KEY_ID='...'; $env:AWS_SECRET_ACCESS_KEY='...'; $env:AWS_SESSION_TOKEN='...'
$env:AWS_REGION='us-east-1'
$env:ADMIN_API_KEY='clave-demo'
.\mvnw.cmd -f admin-service\pom.xml spring-boot:run
```
Todas las llamadas a `/api/admin/**` llevan el header `X-Admin-Key: <ADMIN_API_KEY>`.

## API REST

| Verbo | Ruta | Que hace |
|---|---|---|
| GET | `/api/admin/queues` | Lista las colas |
| POST | `/api/admin/queues` | Crea una cola (con DLQ y Redrive Policy por defecto) |
| GET | `/api/admin/queues/{nombre}/status` | Mensajes disponibles / en proceso / retrasados, retencion, visibility timeout, redrive |
| DELETE | `/api/admin/queues/{nombre}` | Elimina la cola |
| POST | `/api/admin/queues/{nombre}/purge` | Purga la cola |
| GET | `/api/admin/topics` | Lista los topicos |
| POST | `/api/admin/topics` | Crea un topico |
| DELETE | `/api/admin/topics/{nombre}` | Elimina un topico |
| GET | `/api/admin/topics/{nombre}/subscriptions` | Lista las suscripciones del topico |
| POST | `/api/admin/topics/{nombre}/subscriptions` | Suscribe una cola (filtro por `tipoEvento` y raw message delivery) |
| DELETE | `/api/admin/subscriptions?arn=...` | Elimina una suscripcion |
| GET | `/api/admin/consumers` | Estado de los consumidores en cada instancia |
| POST | `/api/admin/consumers/{id}/pause` | Pausa un consumidor (todas las instancias) |
| POST | `/api/admin/consumers/{id}/resume` | Reanuda un consumidor |
| GET | `/api/admin/dlq/{nombre}/status` | Estado de una DLQ |
| POST | `/api/admin/dlq/{nombre}/redrive?max=N` | Reprocesa: mueve los mensajes de la DLQ a su cola de origen |
| POST | `/api/admin/dlq/{nombre}/purge` | Purga la DLQ |

Ids de consumidor: `notificaciones-listener`, `tickets-listener`, `auditoria-listener`.

### Cuerpos
Crear cola (todos los campos menos `nombre` son opcionales; salen de `admin.cola-por-defecto`):
```json
{ "nombre": "pagos-queue", "conDlq": true, "visibilityTimeoutSegundos": 30,
  "retencionSegundos": 86400, "dlqRetencionSegundos": 1209600, "maxReceiveCount": 3 }
```
Crear topico: `{ "nombre": "eventos-prueba" }`

Suscribir una cola:
```json
{ "cola": "pagos-queue", "filtroTipoEvento": ["PEDIDO_CREADO"], "rawMessageDelivery": true }
```

## Errores
`400` datos invalidos, `401` falta X-Admin-Key, `404` cola/topico/consumidor inexistente,
`409` el recurso ya existe, `503` sin conexion o credenciales de AWS.

## Como se pausan los consumidores
Los listeners viven en `pedidos-service`, otro proceso. El administrador llama a
`/internal/consumidores/{id}/pausar|reanudar` de cada instancia listada en `admin.consumidores.urls`
(no se expone por el API Gateway y exige `X-Admin-Key`). Pausar detiene el listener: los mensajes
quedan en la cola y otra instancia activa los toma.

## Valores por defecto (admin.cola-por-defecto)
- `visibility-timeout-segundos: 30` > tiempo maximo de procesamiento de un mensaje.
- `retencion-segundos: 345600` (4 dias): margen para recuperarse de una caida de fin de semana.
- `dlq-retencion-segundos: 1209600` (14 dias, el maximo): la retencion corre desde que el mensaje entro a la cola original, por eso la DLQ debe retener mas.
- `max-receive-count: 3`: 3 intentos antes de enviar a la DLQ.


---

# Administracion de streams (Parcial 3, Kinesis)

Toda la logica esta en `StreamAdminService`; `StreamsAdminController` y `StreamConsumersAdminController` solo delegan.
Se configura en `application.yml` (prefijo `kinesis-admin`): region, valores por defecto (3 shards, 24 h),
ventana de lag (10 min) y las aplicaciones consumidoras que se pueden inspeccionar.

## API REST de streams

| Verbo | Ruta | Que hace |
|---|---|---|
| GET | `/api/admin/streams` | Lista los streams (nombre, estado, modo, shards, retencion) |
| POST | `/api/admin/streams` | Crea un stream (`nombre`; opcionales `shards`, `retencionHoras`, `modo`) |
| GET | `/api/admin/streams/{nombre}` | Describe el stream y sus shards (rango de hash, padre, abierto/cerrado) |
| PUT | `/api/admin/streams/{nombre}/shards` | Cambia la cantidad de shards (`UpdateShardCount`, escalado uniforme) |
| PUT | `/api/admin/streams/{nombre}/retention` | Cambia la retencion en horas (aumenta o reduce) |
| GET | `/api/admin/streams/{nombre}/lag` | Edad maxima del registro leido (`IteratorAgeMilliseconds`, CloudWatch) |
| DELETE | `/api/admin/streams/{nombre}` | Elimina el stream |
| GET | `/api/admin/stream-consumers` | Aplicaciones consumidoras: shards asignados, sin asignar, instancias |
| GET | `/api/admin/stream-consumers/{app}` | Por shard: dueno (instancia), checkpoint, contador de lease y atraso en ms |

Todas llevan `X-Admin-Key`. Ejemplos (PowerShell):
```powershell
$h = @{ 'X-Admin-Key' = 'clave-demo'; 'Content-Type' = 'application/json' }
Invoke-RestMethod http://localhost:8090/api/admin/streams -Headers $h
Invoke-RestMethod http://localhost:8090/api/admin/streams -Method Post -Headers $h -Body '{"nombre":"demo","shards":2}'
Invoke-RestMethod http://localhost:8090/api/admin/streams/pedidos-estado/shards -Method Put -Headers $h -Body '{"shards":6}'
Invoke-RestMethod http://localhost:8090/api/admin/stream-consumers/analitica-app -Headers $h
```

## Como se inspecciona una aplicacion consumidora
KCL guarda, en DynamoDB y en una tabla con el nombre de la aplicacion, una fila por shard con su dueno
(`leaseOwner`: la instancia que lo procesa) y su `checkpoint` (hasta donde esta procesado). El administrador
lee esa tabla, la cruza con los shards abiertos del stream y, por cada checkpoint, lee 1 registro con
`GetRecords` para obtener `MillisBehindLatest` (cuanto falta para llegar al final del shard = el atraso).
Un shard sin fila o sin dueno aparece como `SIN_ASIGNAR`. Las filas de control de KCL (`Leader`,
`Migration3.0`, metricas de workers) se ignoran. Si la aplicacion nunca se inicio, responde 404.

## Limites de AWS que el administrador respeta o informa
- `UpdateShardCount` solo permite como maximo duplicar o reducir a la mitad, y hay un tope de cambios por dia.
  El stream queda en `UPDATING` y luego `ACTIVE`; los shards antiguos se cierran y KCL toma los nuevos solo.
- En el Learner Lab una politica de la organizacion **bloquea aumentar la retencion** (`403`); reducirla si funciona.
  Al crear un stream con mas de 24 h, si AWS lo niega, el stream se crea igual y la respuesta trae una `advertencia`.
- En modo `ON_DEMAND` los shards los administra AWS: el endpoint de shards responde `400`.
- Errores: `400` datos invalidos, `403` operacion no permitida por el entorno, `404` stream o aplicacion inexistente,
  `409` el stream ya existe o esta en cambio, `503` sin credenciales o red.
