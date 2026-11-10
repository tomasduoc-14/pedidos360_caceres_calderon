#!/bin/bash
# =============================================================================
# Crea (o ajusta) los streams de Kinesis de Pedidos360 de forma repetible.
# Se puede ejecutar varias veces: si el stream existe, solo corrige la retencion.
# Ejecutar en AWS CloudShell:  bash crear-streams.sh
# IMPORTANTE: Kinesis cobra por shard y por hora. Al terminar: bash eliminar-streams.sh
# =============================================================================
set -euo pipefail

REGION="us-east-1"

# crear_stream <nombre> <shards> <retencion_en_horas>
crear_stream() {
  local nombre="$1" shards="$2" horas="$3"

  if aws kinesis describe-stream-summary --stream-name "$nombre" --region "$REGION" >/dev/null 2>&1; then
    echo "==> $nombre ya existe, no se crea de nuevo"
  else
    echo "==> Creando $nombre con $shards shard(s)"
    # Modo PROVISIONED: tenemos que fijar los shards (el enunciado pide 3 por stream)
    aws kinesis create-stream --stream-name "$nombre" --shard-count "$shards" --region "$REGION"
    aws kinesis wait stream-exists --stream-name "$nombre" --region "$REGION"
  fi

  local actual
  actual=$(aws kinesis describe-stream-summary --stream-name "$nombre" --region "$REGION" \
    --query 'StreamDescriptionSummary.RetentionPeriodHours' --output text)
  if [ "$horas" -gt "$actual" ]; then
    echo "    retencion: $actual h -> $horas h"
    aws kinesis increase-stream-retention-period --stream-name "$nombre" --retention-period-hours "$horas" --region "$REGION"
  elif [ "$horas" -lt "$actual" ]; then
    echo "    retencion: $actual h -> $horas h"
    aws kinesis decrease-stream-retention-period --stream-name "$nombre" --retention-period-hours "$horas" --region "$REGION"
  else
    echo "    retencion ya es de $horas h"
  fi
}

# --- Flujo 1: estado de los pedidos --------------------------------------------
# 3 shards: paralelismo para 3 instancias de cada aplicacion consumidora.
# 72 h: permite reprocesar los eventos tras un fallo de fin de semana o un error en el codigo.
crear_stream "pedidos-estado" 3 72

# --- Flujo 2: actividad de clientes (vistas y clics) ---------------------------
# 3 shards: mucho volumen de escritura.
# 24 h (valor por defecto): la analitica en linea solo necesita datos recientes.
crear_stream "actividad-clientes" 3 24

# --- Eventos fallidos (dead letter stream) -------------------------------------
# 1 shard: llegan pocos eventos, solo los irrecuperables.
# 168 h (7 dias): debe durar mas que los streams de origen para poder investigar.
crear_stream "eventos-fallidos" 1 168

echo
echo "=== Resumen (nombre, modo, shards, retencion en horas) ==="
for s in pedidos-estado actividad-clientes eventos-fallidos; do
  aws kinesis describe-stream-summary --stream-name "$s" --region "$REGION" \
    --query 'StreamDescriptionSummary.[StreamName,StreamModeDetails.StreamMode,OpenShardCount,RetentionPeriodHours]' \
    --output text
done
