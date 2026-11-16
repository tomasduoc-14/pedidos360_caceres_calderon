#!/bin/bash
# =============================================================================
# Elimina TODO lo que cobra o queda huerfano: streams, alarma y tablas de DynamoDB de las
# aplicaciones consumidoras. Ejecutar en AWS CloudShell:  bash eliminar-recursos.sh
# ANTES: detener con Ctrl+C las aplicaciones consumidoras (si siguen corriendo, recrean sus tablas).
# =============================================================================
set -uo pipefail

REGION="us-east-1"
STREAMS="pedidos-estado actividad-clientes eventos-fallidos"
APPS="notificaciones-app analitica-app tendencias-app"
TABLAS_PROPIAS="analitica-resumen eventos-procesados"
ALARMA="kinesis-pedidos-estado-lag-alto"

existe_tabla() { aws dynamodb describe-table --table-name "$1" --region "$REGION" >/dev/null 2>&1; }

for s in $STREAMS; do
  if aws kinesis describe-stream-summary --stream-name "$s" --region "$REGION" >/dev/null 2>&1; then
    echo "==> Eliminando stream $s"
    aws kinesis delete-stream --stream-name "$s" --enforce-consumer-deletion --region "$REGION"
  else
    echo "    stream $s no existe"
  fi
done

echo "==> Eliminando alarma $ALARMA (si existe)"
aws cloudwatch delete-alarms --alarm-names "$ALARMA" --region "$REGION"

# KCL crea por aplicacion: <app>, <app>-WorkerMetricStats y <app>-CoordinatorState
TABLAS="$TABLAS_PROPIAS"
for a in $APPS; do TABLAS="$TABLAS $a ${a}-WorkerMetricStats ${a}-CoordinatorState"; done
for t in $TABLAS; do
  if existe_tabla "$t"; then
    echo "==> Eliminando tabla $t"
    aws dynamodb delete-table --table-name "$t" --region "$REGION" --query TableDescription.TableStatus --output text
  fi
done

echo
echo "=== Lo que queda (deben ser listas vacias) ==="
aws kinesis list-streams --region "$REGION" --query StreamNames --output text
aws dynamodb list-tables --region "$REGION" --query TableNames --output text
