#!/bin/bash
# =============================================================================
# Elimina los streams de Kinesis de Pedidos360 para dejar de pagar por los shards.
# Ejecutar en AWS CloudShell:  bash eliminar-streams.sh
# =============================================================================
set -euo pipefail

REGION="us-east-1"

for s in pedidos-estado actividad-clientes eventos-fallidos; do
  if aws kinesis describe-stream-summary --stream-name "$s" --region "$REGION" >/dev/null 2>&1; then
    echo "==> Eliminando $s"
    aws kinesis delete-stream --stream-name "$s" --enforce-consumer-deletion --region "$REGION"
  else
    echo "==> $s no existe"
  fi
done

echo
echo "=== Streams que quedan (el borrado puede tardar un minuto) ==="
aws kinesis list-streams --region "$REGION"
