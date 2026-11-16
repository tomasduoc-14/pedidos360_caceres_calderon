#!/bin/bash
# =============================================================================
# Crea (o ajusta) los streams de Kinesis de Pedidos360 de forma repetible.
# Se puede ejecutar varias veces: si el stream existe, solo intenta ajustar la retencion.
# Ejecutar en AWS CloudShell:  bash crear-streams.sh
# IMPORTANTE: Kinesis cobra por shard y por hora. Al terminar: bash eliminar-streams.sh
#
# NOTA SOBRE EL LEARNER LAB: una politica de la organizacion (SCP) bloquea
# kinesis:IncreaseStreamRetentionPeriod. Los valores de retencion de abajo son el
# DISENO; si el entorno no permite aplicarlos, el script avisa y el stream queda en
# 24 h (el valor por defecto de Kinesis). En una cuenta sin esa restriccion se aplican.
# =============================================================================
set -euo pipefail

REGION="us-east-1"

# crear_stream <nombre> <shards> <retencion_deseada_en_horas>
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

  local actual salida
  actual=$(aws kinesis describe-stream-summary --stream-name "$nombre" --region "$REGION" \
    --query 'StreamDescriptionSummary.RetentionPeriodHours' --output text)

  if [ "$horas" -eq "$actual" ]; then
    echo "    retencion ya es de $horas h"
    return 0
  fi

  echo "    retencion: $actual h -> $horas h"
  if [ "$horas" -gt "$actual" ]; then
    salida=$(aws kinesis increase-stream-retention-period --stream-name "$nombre" \
      --retention-period-hours "$horas" --region "$REGION" 2>&1) || {
      if echo "$salida" | grep -q "AccessDenied"; then
        echo "    AVISO: el laboratorio no permite subir la retencion (SCP). Queda en $actual h."
        return 0
      fi
      echo "$salida"
      return 1
    }
  else
    aws kinesis decrease-stream-retention-period --stream-name "$nombre" \
      --retention-period-hours "$horas" --region "$REGION"
  fi
}

# --- Flujo 1: estado de los pedidos --------------------------------------------
# 3 shards: paralelismo para 3 instancias de cada aplicacion consumidora.
# 72 h (diseno): permite reprocesar los eventos tras un fallo de fin de semana.
crear_stream "pedidos-estado" 3 72

# --- Flujo 2: actividad de clientes (vistas y clics) ---------------------------
# 3 shards: mucho volumen de escritura.
# 24 h (valor por defecto): la analitica en linea solo necesita datos recientes.
crear_stream "actividad-clientes" 3 24

# --- Eventos fallidos (dead letter stream) -------------------------------------
# 1 shard: llegan pocos eventos, solo los irrecuperables.
# 168 h (diseno): debe durar mas que los streams de origen para poder investigar.
crear_stream "eventos-fallidos" 1 168

echo
echo "=== Resumen (nombre, modo, shards, retencion real en horas) ==="
for s in pedidos-estado actividad-clientes eventos-fallidos; do
  aws kinesis describe-stream-summary --stream-name "$s" --region "$REGION" \
    --query 'StreamDescriptionSummary.[StreamName,StreamModeDetails.StreamMode,OpenShardCount,RetentionPeriodHours]' \
    --output text
done
