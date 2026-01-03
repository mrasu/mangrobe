# Build context expects the repo root (one level above mangrobe-lab) to include spec/proto.
FROM maven:3 AS build

WORKDIR /workspace/mangrobe-lab/examples-flink
COPY mangrobe-lab/examples-flink/pom.xml /workspace/mangrobe-lab/examples-flink/pom.xml
RUN mvn -B -DskipTests dependency:go-offline

COPY mangrobe-lab/examples-flink/src /workspace/mangrobe-lab/examples-flink/src
COPY spec/proto /workspace/spec/proto

RUN mvn -B -DskipTests package

FROM flink:latest
COPY --from=build /workspace/mangrobe-lab/examples-flink/target/examples-flink-0.1.jar /opt/flink/usrlib/examples-flink-0.1.jar
