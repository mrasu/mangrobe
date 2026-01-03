# Build context expects the repo root (one level above mangrobe-db) to include spec/proto.
FROM maven:3 AS build

WORKDIR /workspace/mangrobe-db/examples-flink
COPY mangrobe-db/examples-flink/pom.xml /workspace/mangrobe-db/examples-flink/pom.xml
RUN mvn -B -DskipTests dependency:go-offline

COPY mangrobe-db/examples-flink/src /workspace/mangrobe-db/examples-flink/src
COPY spec/proto /workspace/spec/proto

RUN mvn -B -DskipTests package

FROM flink:latest
COPY --from=build /workspace/mangrobe-db/examples-flink/target/examples-flink-0.1.jar /opt/flink/usrlib/examples-flink-0.1.jar
