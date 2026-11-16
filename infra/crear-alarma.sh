#!/bin/bash
# =============================================================================
# Alarma de lag de los consumidores + metricas por shard (monitoreo).
# Ejecutar en AWS CloudShell DESPUES de crear los streams:  bash crear-alarma.sh
#
# IteratorAgeMilliseconds = antiguedad del registro mas viejo que el consumidor acaba de leer
# = cuanto va atrasado. Si sube de 60 s durante 2 minutos seguidos, los consumidores no dan abasto
# (hay que agregar instancias, hasta el numero de shards) o estan caidos.
# =============================================================================
set -euo pipefail

REGION="us-east-1"
STREAM="pedidos-estado"
ALARMA="kinesis-${STREAM}-lag-alto"
UMBRAL_MS=60000

echo "==> Activando metricas por shard en $STREAM"
aws kinesis enable-enhanced-monitoring --stream-name "$STREAM" --shard-level-metrics ALL \
  --region "$REGION" --query 'DesiredShardLevelMetrics' --output text

echo "==> Creando alarma $ALARMA (umbral ${UMBRAL_MS} ms, 2 periodos de 60 s)"
aws cloudwatch put-metric-alarm --region "$REGION" \
  --alarm-name "$ALARMA" \
  --alarm-description "Los consumidores van atrasados: el registro mas viejo leido tiene mas de 60 s" \
  --namespace AWS/Kinesis --metric-name GetRecords.IteratorAgeMilliseconds \
  --dimensions Name=StreamName,Value="$STREAM" \
  --statistic Maximum --period 60 --evaluation-periods 2 --datapoints-to-alarm 2 \
  --threshold "$UMBRAL_MS" --comparison-operator GreaterThanOrEqualToThreshold \
  --treat-missing-data notBreaching

aws cloudwatch describe-alarms --alarm-names "$ALARMA" --region "$REGION" \
  --query 'MetricAlarms[].[AlarmName,StateValue,Threshold]' --output text
