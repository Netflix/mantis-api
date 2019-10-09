#!/bin/bash

# create the mantisapi binary
./gradlew clean installDist

# build the docker image
docker build -t netflixoss/mantisapi .

echo "Created Docker image 'netflixoss/mantisapi'"
