#!/bin/bash

echo "--- Iniciando ftp_client en tiempo real ---"
echo "--- Presiona Ctrl+C para detener el nodo ---"

# 3. Ejecutar Docker
# Usamos 'exec' para que las señales (como Ctrl+C) pasen directo al contenedor
exec sudo docker run --rm \
  --network ftp_net \
  -p 8501:8501 \
  -v "$(pwd)/client:/app/client" \
  -v "$(pwd)/downloads:/downloads" \
  --name "client" \
  testing streamlit run /app/client/app.py