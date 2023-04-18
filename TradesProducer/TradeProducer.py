import io
import json
import logging
import os

import avro.io
import avro.schema
import websocket
from kafka import KafkaProducer


def avro_encode(data, schema):
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(data, encoder)
    return bytes_writer.getvalue()


class TradeProducer:
    def __init__(self):
        # Kafka settings
        self.topic = os.getenv('KAFKA_TOPIC_NAME')
        # setting up Kafka producer
        self.producer = KafkaProducer(bootstrap_servers=os.getenv(
            'KAFKA_SERVER') + ':' + os.getenv('KAFKA_PORT'))
        # parse Stocks Ticekrs list from env variable
        self.stock_tickers = json.loads(os.getenv('FINNHUB_STOCKS_TICKERS'))
        self.avro_schema = avro.schema.parse(open('trades.avsc').read())
        # websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp(f'wss://ws.finnhub.io?token={os.environ["FINNHUB_API_KEY"]}',
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close)
        self.ws.on_open = self.on_open
        self.ws.run_forever()

    def on_message(self, ws, message):
        message = json.loads(message)
        # print(message['data'])
        avro_message = avro_encode(
            {
                'data': message['data'],
                'type': message['type']
            },
            self.avro_schema
        )
        # print(avro_message)
        self.producer.send(self.topic, avro_message)

    def on_error(self, ws, error):
        print(error)

    def on_close(self, ws):
        print("### closed ###")

    def on_open(self, ws):
        for ticker in self.stock_tickers:
            try:
                ws.send('{"type":"subscribe","symbol":"%s"}' % ticker)
                print(f'Subscription for {ticker} succeeded')
            except Exception as e:
                print(e)


if __name__ == "__main__":
    producer = TradeProducer()
