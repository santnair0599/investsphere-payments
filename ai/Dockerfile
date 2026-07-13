# Enterprise Decision Agent — container image for Azure Container Apps.
FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

COPY ai/requirements.txt /app/ai/requirements.txt
RUN pip install --no-cache-dir -r /app/ai/requirements.txt

# copy the agent package (and policy corpus for RAG indexing)
COPY ai /app/ai

EXPOSE 8000
# uvicorn serves the FastAPI app; APIM sits in front for auth/rate-limit/metering
CMD ["uvicorn", "ai.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
