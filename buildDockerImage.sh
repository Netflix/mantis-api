#!/bin/bash

# create the mantisapi binary
./gradlew clean installDist

# build the docker image
docker build -t dev/mantisapi .

echo "Created Docker image 'dev/mantisapi'"
