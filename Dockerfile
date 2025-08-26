FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Installer les deps Python uniquement
RUN pip install --no-cache-dir \
    "Flask>=3.0.0" \
    "requests>=2.31.0" \
    "gunicorn>=21.2.0"

# Copie du code (adapte les chemins)
WORKDIR /app
COPY . /app

# Expose le port Flask/Gunicorn
EXPOSE 8000

# Lance l’app Flask (module: app.py avec app=Flask(...))
CMD ["gunicorn","-b","0.0.0.0:8000","--log-level","debug","--access-logfile","-","--error-logfile","-","--capture-output","app:app"]
