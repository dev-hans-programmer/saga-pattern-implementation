FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/workspace/common:/workspace/service
WORKDIR /workspace
COPY common /workspace/common
COPY services/payment/requirements.txt /workspace/service/requirements.txt
RUN pip install --no-cache-dir -r /workspace/service/requirements.txt
COPY services/payment /workspace/service
WORKDIR /workspace/service
RUN useradd --create-home appuser && chown -R appuser:appuser /workspace
USER appuser
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

