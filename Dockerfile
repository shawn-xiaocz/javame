FROM gradle:8.14.3-jdk17 AS build
USER root
WORKDIR /workspace
COPY settings.gradle build.gradle ./
COPY api ./api
COPY src ./src
RUN gradle bootJar --no-daemon

FROM eclipse-temurin:17-jre
WORKDIR /app
RUN groupadd --system javame && useradd --system --gid javame --home-dir /app javame
COPY --from=build --chown=javame:javame /workspace/build/libs/javame-1.0.0.jar /app/javame.jar
USER javame
EXPOSE 8080 8088
ENTRYPOINT ["java", "-jar", "/app/javame.jar"]
