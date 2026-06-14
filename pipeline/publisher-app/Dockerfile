FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY ais_publisher.py .
ENV PYTHONUNBUFFERED=1
CMD ["python3", "-u", "ais_publisher.py"]
