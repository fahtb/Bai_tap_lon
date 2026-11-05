FROM apache/spark:3.5.3-python3@sha256:d0f5d63bfbbffdbec08debb4911554ba671a2fa7398374db8db314d80328f576
USER root
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN python3 -m pip install --no-cache-dir -r requirements.txt
ENV PYTHONPATH=/app PYTHONUNBUFFERED=1
COPY . /app
ENTRYPOINT []
CMD ["python3", "-m", "generator.api"]
