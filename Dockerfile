FROM openjdk:8-jre

COPY target/rabbitmq-1.0-SNAPSHOT-jar-with-dependencies.jar /experiment.jar

ENTRYPOINT java -jar /experiment.jar