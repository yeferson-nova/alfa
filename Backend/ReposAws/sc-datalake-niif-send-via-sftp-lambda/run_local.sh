#!/bin/bash

docker build -t glue-local .
docker run -v $(pwd)/scripts:/home/glue/scripts -it glue-local