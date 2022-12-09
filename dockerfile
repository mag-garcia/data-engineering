FROM tiangolo/uvicorn-gunicorn-fastapi:python:3.10.7
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY . /app