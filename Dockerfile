FROM maven:3.5-jdk-8
ENV MAXWELL_VERSION=1.10.9 KAFKA_VERSION=0.11.0.1

COPY . /workspace

RUN apt-get update \
    && apt-get -y upgrade \
    && apt-get install -y build-essential

ENV WORKSPACE=/workspace
WORKDIR $WORKSPACE
ADD pom.xml .
RUN ["mvn", "verify", "clean", "--fail-never"]
ADD . $WORKSPACE
RUN ["mvn", "package", "-DskipTests=true"]
RUN mkdir /app \
    && mv $WORKSPACE/target/maxwell-$MAXWELL_VERSION/maxwell-$MAXWELL_VERSION/* /app/ \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* /usr/share/doc/* $WORKSPACE/

WORKDIR /app

RUN echo "$MAXWELL_VERSION" > /REVISION
CMD bin/maxwell --user=$MYSQL_USERNAME --password=$MYSQL_PASSWORD --host=$MYSQL_HOST --producer=$PRODUCER $MAXWELL_OPTIONS