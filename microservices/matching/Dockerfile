FROM python:3.8-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY src/ .
# RUN apt-get update && apt-get install -y redis-server
EXPOSE 5000
CMD ["python", "matching.py"]
