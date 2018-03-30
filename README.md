# DistributedProcessing

This is a simple example of using Apache Flink to perform stream processing

The sample can be summarized as follows :-
- Accept stream of inputs
- Perform deduplication within a time window
- Enrich the stream using Asynchronous web service calls
- Transform the response
- Write the output to RollingFile sink and Elasticsearch sink
