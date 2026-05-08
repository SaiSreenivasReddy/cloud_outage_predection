FROM python:3.12-slim

WORKDIR /app

# Copy the single root-level requirements file
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy only the webapp source code
COPY webapp/ .

CMD ["streamlit", "run", "1_Prediction.py", "--server.port=8501", "--server.address=0.0.0.0"]
