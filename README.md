# ISOBlue data microservice
This microservice consumes ISOBlue Kafka messages and store them to the specified OADA server.
The domain of Kafka broker and the URL of the OADA server are specified in `defaultenv.list`.

This microservice runs as a Docker container. To build a container, run:
``` shell
docker build -t isoblue-uservice .
```

To start the container, run:
``` shell
docker run --env-file defaultenv.list isoblue-uservice
```
