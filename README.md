# Beam Model server

This is a simple implementation of model serving using Beam
The basic idea behind this implementation is fairly straightforward - there are two streams:

-**Data Stream** - Kafka stream delivering data record as protobuf buffer (example, modeldescriptor.proto)

-**Model Stream** - Kafka stream delivering models as protobuf buffer (example, modeldescriptor.proto)

The overall implementation that brings together both streams and then processes
every input of this stream. It is also using [state](https://beam.apache.org/blog/2017/02/13/stateful-processing.html)
to store an intermediate value of the model.

This project contains the following modules:

-**data** - a directory of data files used as sources for all applications

-**protobufs** - a module containing protobufs that are used for all streaming frameworks. 
This protobufs describe model and data definition in the stream. Because Beam is Java 
and the rest of implementations are Scala, both Java and Scala implementations of protobufs are 
generated

-**client** - generic client used for testing implementation
Reads data files, split them into records, converts to protobuf implementations and publishes them to Kafka

-**configuration** - simple module containing class with Kafka definitions - server location,
topics, etc. used by all applications

-**server** - implementation of model scoring using Beam. Both Java and Scala implementations are provided.

