FROM python:3.8.10-slim
WORKDIR /app
ADD requirements.txt .
RUN apt update && apt install -y binutils wget \
  && pip install -r requirements.txt \
  && rm -rf /var/lib/apt/lists/* && rm -rf ~/.cache/pip/*
ADD main.py .
ADD public ./public
EXPOSE 8123
CMD ["python3","./main.py"]
