FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# pandas needs libstdc++ and basic build tools for wheels fallback
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
# Copy CE scorer into image under multiple names to satisfy dynamic loader
COPY CE_RANKING.PY ./
COPY CE_RANKING.PY ./ce_rank.py
# Sanity check during build so we fail fast if file is missing
RUN test -f CE_RANKING.PY && echo "CE_RANKING.PY present"

EXPOSE 8000

# Default command runs API; worker service overrides this in compose
CMD ["uvicorn", "app.app:app", "--host", "0.0.0.0", "--port", "8000"]


