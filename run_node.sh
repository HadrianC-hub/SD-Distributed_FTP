#!/bin/bash

# 1. Validar que se pasó el número del nodo
if [ -z "$1" ]; then
    echo "Error: Indica el número del nodo."
    echo "Uso: $0 <X>"
    exit 1
fi

X=$1

# 2. Informar al usuario
echo "--- Iniciando ftp_node_$X en tiempo real ---"
echo "--- Presiona Ctrl+C para detener el nodo ---"

# 3. Ejecutar Docker
# Usamos 'exec' para que las señales (como Ctrl+C) pasen directo al contenedor
exec sudo docker run --rm \
  --network ftp_net \
  --network-alias ftp_cluster \
  -p "2${X}21:21" \
  -p "210${X}0-210${X}5:21000-21005" \
  -v "$(pwd)/server-distributed:/app/server" \
  -v "$(pwd)/storage_node_${X}:/app/server/data" \
  --name "ftp_node_${X}" \
  -e NODE_ID="${X}" \
  testing python -u /app/server/server.py