FROM jupyter/pyspark-notebook:spark-3.3.2

WORKDIR /TradesProcessor
COPY . .

RUN pip install -r requirements.txt

CMD ["python","TradesProcessor.py"] 