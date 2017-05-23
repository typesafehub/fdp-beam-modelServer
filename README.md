# Beam Model server

This is a simple implementation of model serving using Beam
The basic idea behind this implementation is fairly straightforward - there are two streams:

-**Data Stream** - Kafka stream delivering data record as protobuf buffer (example, modeldescriptor.proto)

-**Model Stream** - Kafka stream delivering models as protobuf buffer (example, modeldescriptor.proto)

The pproper implementation is ModelServer1. It brings together both streams and then processes
every input of this stream. It is also using state (as described at https://beam.apache.org/blog/2017/02/13/stateful-processing.html)
to store an intermediate value of the model.