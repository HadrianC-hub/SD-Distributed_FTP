#!/bin/bash

echo "🚀 Iniciando despliegue del sistema FTP..."

# Crear directorios necesarios
sudo mkdir -p server/data client/downloads
sudo chown -R $USER:$USER server/data client/downloads

echo "📦 Construyendo imágenes Docker..."
sudo docker-compose build

echo "🐳 Iniciando contenedores..."
sudo docker-compose up -d

echo "✅ Despliegue completado!"
echo ""
echo "🌐 Acceso a la aplicación:"
echo "   Cliente FTP: http://localhost:8501"
echo ""
echo "🔧 Credenciales de prueba:"
echo "   usuario: user  | contraseña: pass"
echo "   usuario: user1 | contraseña: pass1"
echo "   ... hasta user5"
echo ""
echo "📊 Para ver los logs: sudo docker-compose logs -f"
echo "🛑 Para detener: sudo docker-compose down"
