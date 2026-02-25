# Stage 1 - Build avec Maven
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app

# Cache deps
COPY pom.xml .
RUN mvn -q -B -DskipTests dependency:go-offline

# Build
COPY src ./src
RUN mvn -q -B -DskipTests clean package

# Stage 2 - Runtime avec SSH client
FROM eclipse-temurin:17-jre
WORKDIR /app

# Installer openssh-client
RUN apt-get update && \
    apt-get install -y openssh-client && \
    rm -rf /var/lib/apt/lists/*

COPY --from=build /app/target/*.jar app.jar

EXPOSE 8080

ENTRYPOINT ["java","-jar","app.jar"]