FROM openjdk:17-jdk-slim
EXPOSE 8080
ADD /target/litecdc-0.0.1-SNAPSHOT.jar litecdc-0.0.1-SNAPSHOT.jar
ENV JAVA_OPTS="-Xmx8g"
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar litecdc-0.0.1-SNAPSHOT.jar"]