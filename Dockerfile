FROM azul/zulu-openjdk:8-latest

MAINTAINER Mantis Developers <mantis-oss-dev@netflix.com>

COPY ./build/install/mantis-api/bin/* /apps/nfmantisapi/bin/
COPY ./build/install/mantis-api/lib/* /apps/nfmantisapi/lib/

RUN mkdir -p /apps/nfmantisapi/mantisArtifacts
RUN mkdir -p /logs/mantisapi

WORKDIR /apps/nfmantisapi

ENTRYPOINT [ "bin/mantis-api", "-p", "api-docker.properties" ]
