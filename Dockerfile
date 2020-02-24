FROM openjdk:8
ADD target/nasa-log-analyzer-1.0-SNAPSHOT-jar-with-dependencies.jar nasa-log-analyzer-1.0-SNAPSHOT-jar-with-dependencies.jar
ENTRYPOINT ["java", "-jar", "nasa-log-analyzer-1.0-SNAPSHOT-jar-with-dependencies.jar"]
