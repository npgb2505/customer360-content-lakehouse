FROM apache/spark:4.0.0-python3

USER root
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip
WORKDIR /app

COPY requirements-container.txt ./
RUN python3 -m pip install --no-cache-dir -r requirements-container.txt

COPY customer360 ./customer360
COPY . .
ENTRYPOINT ["python3", "-m", "customer360.cli"]
CMD ["demo", "--root", "/app/data"]
