FROM java:8
MAINTAINER Mantis Developers <mantis-oss-dev@netflix.com>

COPY ./build/install/mantis-api/bin/* /apps/nfmantisapi/bin/
COPY ./build/install/mantis-api/lib/* /apps/nfmantisapi/lib/

COPY ./conf/local.properties /apps/nfmantisapi/conf/

WORKDIR /apps/nfmantisapi

ENTRYPOINT [ "bin/mantis-api", "-p", "conf/local.properties" ]
