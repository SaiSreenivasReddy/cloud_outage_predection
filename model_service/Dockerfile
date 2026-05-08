FROM python:3.12-slim

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the single root-level requirements file
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy only the model_service source code
COPY model_service/ .

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
