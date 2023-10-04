FROM openjdk:17-jdk-slim
EXPOSE 8080
ADD /target/change-data-capture-application-0.0.1-SNAPSHOT.jar change-data-capture-application-0.0.1-SNAPSHOT.jar
ENTRYPOINT ["java","-jar","change-data-capture-application-0.0.1-SNAPSHOT.jar"]