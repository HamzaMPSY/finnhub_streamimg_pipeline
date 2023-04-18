# Finnhub Streaming Pipeline

The Finnhub Streaming Pipeline is a real-time data pipeline that uses the Finnhub.io API/websocket to stream trading data. The purpose of this project is to showcase key aspects of streaming pipeline development and architecture, providing low latency, scalability, and availability.

The project is inspired by the  [finnhub-streaming-data-pipeline](https://github.com/RSKriegs/finnhub-streaming-data-pipeline) repository, but I have redesigned and optimized the code myself.
## Architecture

The architecture of the pipeline is shown in the diagram below:

![finnhub_streaming_pipeline](/architecture.png)

The diagram provides a detailed insight into the pipeline's architecture.

## Progress
Currently, the progress of the project is as follows:

1. A trades producer container using Kafka and Finnhub API with sockets.
2. A trades processor container that uses Spark as a Kafka consumer and injects it into Cassandra.
3. ... (Further progress is yet to be specified.)