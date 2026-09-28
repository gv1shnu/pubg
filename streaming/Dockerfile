FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY streaming/pom.xml streaming/pom.xml
COPY contracts contracts
COPY streaming/src streaming/src
RUN --mount=type=cache,target=/root/.m2 mvn -B -f streaming/pom.xml verify
FROM flink:1.20.1-scala_2.12-java17
COPY --from=build /build/streaming/target/battleground-stream-0.1.0-dev.2.jar /opt/flink/usrlib/telemetry.jar
