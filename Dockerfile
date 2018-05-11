FROM maven:3.5-jdk-8
ENV MAXWELL_VERSION=1.14.4 KAFKA_VERSION=0.11.0.1

COPY . /workspace

RUN mkdir /app \
    && mv /workspace/target/maxwell-$MAXWELL_VERSION/maxwell-$MAXWELL_VERSION/* /app/ \
    && echo "$MAXWELL_VERSION" > /REVISION

WORKDIR /app
CMD bin/maxwell --user=$MYSQL_USERNAME --password=$MYSQL_PASSWORD --host=$MYSQL_HOST --producer=$PRODUCER $MAXWELL_OPTIONS