FROM openjdk:17-jdk-slim
EXPOSE 8080
ADD /target/change-data-capture-application-0.0.1-SNAPSHOT.jar change-data-capture-application-0.0.1-SNAPSHOT.jar
ENV JAVA_OPTS="-Xmx8g"
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar change-data-capture-application-0.0.1-SNAPSHOT.jar"]