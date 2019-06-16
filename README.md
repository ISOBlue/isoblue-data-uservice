# ISOBlue Data Microservice

ISOBlue Data Microservice works as an interface between [ISOBlue](https://www.isoblue.org) devices and an [OADA Server](https://github.com/OADA/oada-srvc-docker/).
The microservice consumes ISOBlue Kafka messages from the specified broker and store them to the OADA server.

## Prerequisites

You will need:

* Docker
* An OADA Server with write access to `oada.isoblue` scope type
* A Kafka broker

## Getting Started

First, clone the repository:

``` shell
git clone https://github.com/ISOBlue/isoblue-data-uservice.git
```

This microservice runs as a Docker container. To build the container, open the top directory of the cloned repository (i.e., `cd isoblue-data-uservice`) and run:
``` shell
docker build -t isoblue-uservice .
```

The URL to the OADA server and the Kafka broker are specified in `defaultenv.list` file.
Open the `defaultenv.list` file and update the parameters.

To start the container, run:
``` shell
docker run --env-file defaultenv.list isoblue-uservice
```

## Notes
* For development, you may need to set `NODE_TLS_REJECT_UNAUTHORIZED=0` in `defaultenv.list` to ignore self-signed certificates. (WARNING: This is insecure!)
