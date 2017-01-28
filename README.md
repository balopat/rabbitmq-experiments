# RabbitMQ network partitioning experiments with Docker


To run: 

````
 docker pull balopat/rabbitmq-cluster-manager:latest
 docker pull balopat/rabbitmq-partitioning-experiment
 docker run --net=host -v /var/run/docker.sock:/var/run/docker.sock balopat/rabbitmq-partitioning-experiment 
 ````

To build: 

````
 mvn assembly:assembly; docker build -t balopat/rabbitmq-partitioning-experiment .
````

Depends on: https://github.com/balopat/testing-rabbitmq-clustering-with-docker (forked from GetLevvel/testing-rabbitmq-clustering-with-docker)

