FROM python:3.9-slim

WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    && rm -rf /var/lib/apt/lists/*

# Copiar archivos de la aplicación
COPY app.py .
COPY client.py .
COPY requirements.txt .

# Instalar dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Crear directorio para descargas
RUN mkdir -p downloads

# Exponer puerto de Streamlit
EXPOSE 8501

# Ejecutar la aplicación
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]