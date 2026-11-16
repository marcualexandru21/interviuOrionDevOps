FROM python:3.11-slim AS builder
# Instalam direct in /root/.local
RUN pip install --no-cache-dir --user prometheus-client==0.21.1

FROM python:3.11-slim AS runner
WORKDIR /app

RUN groupadd -g 10001 appgroup \
    && useradd -u 10001 -g appgroup -m -s /bin/bash appuser

COPY --from=builder /root/.local /home/appuser/.local
COPY pyApp/app.py .

RUN chown -R appuser:appgroup /app /home/appuser/.local
USER 10001

ENV PATH=/home/appuser/.local/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    METRICS_PORT=8000

EXPOSE 8000
CMD ["python", "app.py"]