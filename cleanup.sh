#!/bin/bash

echo "🧹 Limpiando contenedores y volúmenes..."
docker-compose down -v

echo "🗑️ Eliminando imágenes..."
docker rmi $(docker images "ftp-*" -q) 2>/dev/null || echo "No hay imágenes para eliminar"

echo "✅ Limpieza completada!"