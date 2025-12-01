# Imagen base ligera
FROM python:3.9-slim

# Directorio de trabajo (donde está server.py)
WORKDIR /app/server

# Instalar dependencias del sistema mínimas
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Copiar todo el contenido del servidor dentro del contenedor
COPY . /app/server

# Crear carpetas esenciales
RUN mkdir -p /app/server/root && \
    touch /app/server/users.json

# Variables de entorno
ENV FTP_ROOT=/app/server
ENV FTP_PASV_PORTS=21000-21100
ENV FTP_PASV_MIN_PORT=21000
ENV FTP_PASV_MAX_PORT=21100

# Exponer puertos FTP y modo pasivo
EXPOSE 21 21000-21100

# Comando de inicio
CMD ["python", "server.py"]
