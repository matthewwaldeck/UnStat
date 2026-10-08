FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py index.html ./
ENV DATA_DIR=/data
VOLUME /data
EXPOSE 8080
# One worker on purpose: the collector thread starts with the app and must run only once.
CMD ["gunicorn", "-b", "0.0.0.0:8080", "-w", "1", "--threads", "4", "app:app"]
