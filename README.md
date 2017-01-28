# RabbitMQ network partitioning experiments with Docker


To run: 

````
 docker pull balopat/rabbitmq-cluster-manager:latest
 docker pull balopat/rabbitmq-partitioning-experiment
 docker run --net=host -v /var/run/docker.sock:/var/run/docker.sock balopat/rabbitmq-partitioning-experiment 
 ````
To inspect the containers at the end of it, run it without cleanup use the `-no_cleanup` flag: 

````
 docker run --net=host -v /var/run/docker.sock:/var/run/docker.sock balopat/rabbitmq-partitioning-experiment -no_cleanup
````

To do a dry run with no partitioning, run it with `-no_partitioning` flag: 

````
 docker run --net=host -v /var/run/docker.sock:/var/run/docker.sock balopat/rabbitmq-partitioning-experiment -no_cleanup
````



To build: 

````
 mvn assembly:assembly; docker build -t balopat/rabbitmq-partitioning-experiment .
````

Depends on: https://github.com/balopat/testing-rabbitmq-clustering-with-docker (forked from GetLevvel/testing-rabbitmq-clustering-with-docker)

