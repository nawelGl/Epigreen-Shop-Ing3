# Build de l'application Maven
FROM maven:3.9-eclipse-temurin-21 AS builder
WORKDIR /app
# copie le code source et le pom.xml
COPY pom.xml .
COPY src ./src
# On compile 
RUN mvn clean package -DskipTests

# Création de l'image 
FROM eclipse-temurin:21-jre
WORKDIR /app
# récupèrer le .jar généré 
COPY --from=builder /app/target/*.jar app.jar

# ancer l'application
ENTRYPOINT ["java", "-jar", "app.jar"]