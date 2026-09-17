# Build stage: compiles the jar. Dependencies are resolved in their own layer (before
# `COPY src`) so an unchanged pom.xml lets Docker reuse the cached dependency download
# across rebuilds, instead of re-downloading everything whenever only source changes.
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -q dependency:go-offline
COPY src ./src
RUN mvn -q -DskipTests package

# Runtime stage: JRE only (no Maven/JDK), non-root user.
FROM eclipse-temurin:17-jre-alpine
RUN addgroup -S app && adduser -S app -G app
WORKDIR /app
COPY --from=build /build/target/payment-event-service-*.jar app.jar
USER app
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
