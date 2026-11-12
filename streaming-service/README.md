# streaming-service

Aplicaciones consumidoras de **Amazon Kinesis Data Streams**, con la Kinesis Client Library (KCL) 3.x.
Forma parte de Pedidos360 (Parcial 3). Los productores viven en `pedidos-service`; la administracion de
streams, en `admin-service`.

## Aplicaciones consumidoras

| Aplicacion (consumer group) | Stream que lee | Que hace |
|---|---|---|
| `notificaciones-app` | `pedidos-estado` | Avisa al cliente en cada cambio de estado del pedido |
| `analitica-app` | `pedidos-estado` | Totales en linea por tipo de evento y por producto |
| `tendencias-app` | `actividad-clientes` | Vistas y agregados al carro por producto |

`notificaciones-app` y `analitica-app` leen el **mismo stream** y cada una lleva su propio avance (checkpoint).
Cada nombre de aplicacion es tambien el nombre de su **tabla de coordinacion en DynamoDB**: ahi la KCL guarda
que instancia tiene cada shard y hasta que sequence number se leyo. Las instancias con el mismo nombre se
reparten los shards, y si una cae, otra toma los suyos.

## Recorrido de un evento

```
shard -> ProcesadorShard (KCL) -> ProcesadorEventos
   1. deserializar JSON            -> si falla: stream eventos-fallidos
   2. validar el contenido         -> si falla: stream eventos-fallidos
   3. duplicado (eventoId)?        -> se ignora
   4. negocio, con reintentos      -> transitorio: espera creciente (500, 1000, 2000 ms)
                                      irrecuperable o reintentos agotados: stream eventos-fallidos
   5. checkpoint al terminar el lote
```
Un evento defectuoso nunca detiene la lectura del shard. Todos los logs llevan `eventoId`, `intento` y `causa`.

## Configuracion

Todo vive en `src/main/resources/application.yml` (prefijo `streaming`). Variables de entorno utiles:

| Variable | Efecto |
|---|---|
| `STREAMING_HABILITADO=true` | Enciende los consumidores (por defecto, apagado: arranca sin AWS) |
| `STREAMING_APP_NOTIFICACIONES`, `STREAMING_APP_ANALITICA`, `STREAMING_APP_TENDENCIAS` | `false` apaga esa aplicacion en esta instancia |
| `STREAMING_SIMULAR_FALLOS=false` | Desactiva los productos de demostracion de fallos |
| `AWS_REGION` | Region (por defecto `us-east-1`) |

Credenciales: cadena por defecto del SDK (variables de entorno en el PC, rol de IAM en el EC2). Nunca en el repositorio.

## Fallos de demostracion

Se activan al crear un pedido con estos productos (solo en `notificaciones-app`):
`FALLO_TRANSITORIO` (falla 2 veces y luego funciona), `FALLO_PERSISTENTE` (agota los reintentos y va a
fallidos) y `FALLO_IRRECUPERABLE` (va directo a fallidos). `analitica-app` procesa esos mismos eventos
sin problema: son aplicaciones independientes.

## Ejecutar

```
.\mvnw.cmd -f streaming-service\pom.xml clean compile
.\mvnw.cmd -f streaming-service\pom.xml test
$env:STREAMING_HABILITADO='true'
.\mvnw.cmd -f streaming-service\pom.xml spring-boot:run
```
Para ver el reparto de shards, levanta una segunda instancia con otro puerto:
`.\mvnw.cmd -f streaming-service\pom.xml spring-boot:run "-Dspring-boot.run.arguments=--server.port=8071"`.

## Estado en linea

- `GET /api/analitica/pedidos` : eventos por tipo y unidades por producto.
- `GET /api/analitica/tendencias` : vistas y agregados al carro por producto.
