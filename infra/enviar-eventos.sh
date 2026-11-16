#!/bin/bash
# =============================================================================
# Envia eventos de prueba al stream pedidos-estado (partition key = pedidoId).
# Ejecutar en AWS CloudShell:
#   bash enviar-eventos.sh base            # 6 normales + duplicado + los 4 casos de error
#   bash enviar-eventos.sh lote 401 30     # 30 eventos normales desde el pedidoId 401
# Cada linea impresa es "ShardId  SequenceNumber" del evento enviado.
# Los productos FALLO_* los reconoce streaming-service cuando STREAMING_SIMULAR_FALLOS=true.
# =============================================================================
set -euo pipefail

REGION="us-east-1"
STREAM="pedidos-estado"

enviar() { # $1 = partition key, $2 = json
  aws kinesis put-record --stream-name "$STREAM" --region "$REGION" --partition-key "$1" \
    --cli-binary-format raw-in-base64-out --data "$2" \
    --query '[ShardId,SequenceNumber]' --output text
}

evento() { # id pedidoId producto cantidad email
  local ahora; ahora=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  echo "{\"eventoId\":\"$1\",\"tipo\":\"PEDIDO_CREADO\",\"pedidoId\":$2,\"producto\":\"$3\",\"cantidad\":$4,\"clienteEmail\":\"$5\",\"estado\":\"PENDIENTE\",\"ocurridoEn\":\"$ahora\"}"
}

case "${1:-base}" in
  base)
    echo "# 1) Eventos normales"
    for i in 101 102 103 104 105 106; do enviar $i "$(evento ok-$i $i Zapatillas 2 cliente$i@mail.com)"; done
    echo "# 2) Duplicado: mismo eventoId enviado dos veces"
    enviar 101 "$(evento ok-101 101 Zapatillas 2 cliente101@mail.com)"
    echo "# 3) Error transitorio: falla 2 veces y luego funciona"
    enviar 201 "$(evento trans-201 201 FALLO_TRANSITORIO 1 a@mail.com)"
    echo "# 4) Error persistente: agota los reintentos y va a eventos-fallidos"
    enviar 202 "$(evento pers-202 202 FALLO_PERSISTENTE 1 b@mail.com)"
    echo "# 5) Error irrecuperable: va directo a eventos-fallidos"
    enviar 203 "$(evento irrec-203 203 FALLO_IRRECUPERABLE 1 c@mail.com)"
    echo "# 6) Evento invalido (validacion): email malo y cantidad 0"
    enviar 204 "$(evento inval-204 204 Zapatillas 0 no-es-email)"
    ;;
  lote)
    desde="${2:-401}"; cantidad="${3:-30}"
    for ((i = desde; i < desde + cantidad; i++)); do
      enviar $i "$(evento lote-$i $i Polera 1 c$i@mail.com)" >/dev/null
    done
    echo "listo: $cantidad eventos (lote-$desde ...)"
    ;;
  *)
    echo "Uso: bash enviar-eventos.sh base | lote [desde] [cantidad]"; exit 1 ;;
esac
